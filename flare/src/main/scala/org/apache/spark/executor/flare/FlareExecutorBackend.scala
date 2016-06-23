package org.apache.spark.executor.flare

import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.flare._
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.flare._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.flare.FlareMessages._
import org.apache.spark.scheduler.flare._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.{SignalLogger, ThreadUtils}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

private[spark] class FlareExecutorBackend(
    executorId: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv,
    proxyRef: RpcEndpointRef,
    cluster: FlareCluster)
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  case object AttemptLaunchReservation
  case class RemoveRunningTask(reservationId: FlareReservationId)
  
  logInfo("Starting Flare Executor Backend")
  
  override val rpcEnv = env.rpcEnv

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("flare-executor-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val driverEndpoints = new mutable.HashMap[FlareReservationId, RpcEndpointRef]

  private val rootPool = FlareReservationPool.createRootPool(cluster)

  private val taskToReservationId = new mutable.HashMap[Long, FlareReservationId]
  private val reservationTasks = new HashMap[FlareReservationId, Set[Long]] with MultiMap[FlareReservationId, Long]

  private val activeTasks = new AtomicInteger()

  private val attemptLaunchScheduled = new AtomicBoolean()
  private val attemptLaunchScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("flare-executor-attempt-launch-scheduler")

  private def handleReservation(reservation: FlareReservation) = {
    logDebug(s"Received new reservation: $reservation")

    driverEndpoints(reservation.reservationId) = reservation.driverEndpoint
    rootPool.addReservation(reservation.reservationId, reservation.count, reservation.groups)
    
    attemptLaunchReservation()
  }

  private def cancelReservation(reservationId: FlareReservationId) = {
    logDebug(s"Cancelling reservation $reservationId")

    reservationTasks.get(reservationId).foreach(_.foreach(killTask(_, false)))

    rootPool.removeReservation(reservationId)

    attemptLaunchReservation()
  }

  private def killTask(taskId: Long, interrupt: Boolean) = {
    executor.killTask(taskId, interrupt)

    taskToReservationId.remove(taskId).map {
      reservationId => {
        rootPool.removeRunningTask(reservationId)
        reservationTasks.removeBinding(reservationId, taskId)
      }
    }
  }

  private def scheduleAttemptLaunchReservation(): Unit = {
    if (!attemptLaunchScheduled.get) {
      attemptLaunchScheduler.schedule(
        new Runnable {
          def run() = {
            attemptLaunchReservation()
            attemptLaunchScheduled.set(false)
          }
        }, 300, TimeUnit.MILLISECONDS)
      attemptLaunchScheduled.set(true)
    }
  }

  private def attemptLaunchReservation(): Unit = {
    self.send(AttemptLaunchReservation)
  }

  private def removeRunningTask(reservationId: FlareReservationId) = {
    self.send(RemoveRunningTask(reservationId))
  }

  private def launchReservation(reservationId: FlareReservationId): Unit = {
    activeTasks.incrementAndGet()
    rootPool.addRunningTask(reservationId)

    val driverEndpoint = driverEndpoints(reservationId)
    driverEndpoint.ask[RedeemReservationResponse](RedeemReservation(reservationId, executorId, hostname)) onComplete {
      case Success(response) => response match {
        case SkipReservationResponse =>
          removeRunningTask(reservationId)
          attemptLaunchReservation()
        case LaunchTaskReservationResponse(taskData) => 
          val taskDesc = ser.deserialize[TaskDescription](taskData.value)
          taskToReservationId(taskDesc.taskId) = reservationId
          reservationTasks.addBinding(reservationId, taskDesc.taskId)
          executor.launchTask(this, taskDesc.taskId, taskDesc.attemptNumber, taskDesc.name, taskDesc.serializedTask)
        case _ =>
      }
      case Failure(failure) =>
        removeRunningTask(reservationId)
        attemptLaunchReservation()
    }
  }
  
  var executor: Executor = _
  
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()
  
  override def onStart() = {
    proxyRef.askWithRetry[RegisteredExecutorResponse](RegisterExecutor(executorId, self, cores, Map.empty)) match {
      case RegisteredExecutor => {
        executor = new Executor(executorId, hostname, env, List.empty, isLocal = false)
      }
      case RegisterExecutorFailed(msg) => {
        logError(s"Error registering executor: $msg")
        throw new SparkException(s"Error registering executor: $msg")
      }
    }
  }
  
  override def onDisconnected(remoteAddress: RpcAddress): Unit = {

  }
  
  override def statusUpdate(taskId: Long, state: TaskState.TaskState, data: ByteBuffer) = {
    val msg = StatusUpdate(executorId, taskId, state, data)
    proxyRef.send(msg)
    
    if (TaskState.isFinished(state)) {
      val reservationId = taskToReservationId(taskId)
      removeRunningTask(taskToReservationId(taskId))
      taskToReservationId.remove(taskId)
      reservationTasks.removeBinding(reservationId, taskId)

      attemptLaunchReservation()
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case reservation: FlareReservation => handleReservation(reservation)
    case CancelReservation(reservationId) => cancelReservation(reservationId)
    case AttemptLaunchReservation => {
      if (activeTasks.get < cores) {
        val startTime = System.currentTimeMillis()
        val nextReservation = rootPool.nextReservation
        val duration = System.currentTimeMillis() - startTime
        logDebug(s"nextReservation took $duration ms, has result: ${nextReservation.isDefined}")

        nextReservation match {
          case Some(reservationId) => {
            launchReservation(reservationId)
            attemptLaunchReservation()
          }
          case None => {
            scheduleAttemptLaunchReservation()
          }
        }
      }
    }
    case RemoveRunningTask(reservationId) => {
      activeTasks.decrementAndGet()
      rootPool.removeRunningTask(reservationId)
    }
    case _ =>
  }  
}

private[spark] object FlareExecutorBackend extends Logging {
  val ENDPOINT_NAME = "Executor"
  val PROXY_PORT = 21000
  val EXECUTOR_PORT = 20000

  var cluster: FlareCluster = _

  private def run(
    executorId: String,
    cores: Int,
    clusterConf: FlareClusterConfiguration) {
    
    SignalLogger.register(log)
    
    SparkHadoopUtil.get.runAsSparkUser { () =>
      cluster = FlareCluster(clusterConf)
      cluster.connect()

      val executorConf = new SparkConf

      if (!cluster.state.isInitialized) {
        logInfo("Waiting for driver")
        while (!cluster.state.isInitialized) {
          synchronized {
            this.wait(100)
          }
        }
        logInfo("Cluster initialized")
      }

      logInfo(s"Driver Properties: ${cluster.state.properties}")

      cluster.state.properties.foreach {
        case (key, value) => executorConf.set(key, value)
      }

      executorConf.set("spark.app.id", cluster.state.appId)

      val proxyConf = new SparkConf
      val proxyRpcEnv = RpcEnv.create("sparkDriver", clusterConf.bindHostname, PROXY_PORT, proxyConf, new SecurityManager(proxyConf), false)

      proxyRpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, new FlareHeartbeatProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, new FlareMapOutputTrackerProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME, new FlareBlockManagerProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint("OutputCommitCoordinator", new FlareOutputCommitCoordinatorProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint(FlareSchedulerBackend.ENDPOINT_NAME, new FlareSchedulerProxy(cluster, proxyRpcEnv))

      executorConf.set("spark.driver.host", proxyRpcEnv.address.host)
      executorConf.set("spark.driver.port", proxyRpcEnv.address.port.toString)

      val env = SparkEnv.createExecutorEnv(executorConf, executorId, clusterConf.bindHostname, EXECUTOR_PORT, cores, isLocal = false)
     
      val driverRef = env.rpcEnv.setupEndpointRef("sparkDriver", proxyRpcEnv.address, FlareSchedulerBackend.ENDPOINT_NAME)

      val userClassPath = List.empty[URL]
      
      env.rpcEnv.setupEndpoint(ENDPOINT_NAME, new FlareExecutorBackend(executorId, proxyRpcEnv.address.host, cores, userClassPath, env, driverRef, cluster))
   
      cluster.send(ExecutorLaunched(executorId, proxyRpcEnv.address.host, proxyRpcEnv.address.port))

      env.rpcEnv.awaitTermination()      
    }
  }
  
  def main(args: Array[String]) {
    var executorId: String = null
    var clusterUrl: String = null
    var bindHostname: String = FlareClusterConfiguration.DefaultBindHostname
    var bindPort: Int = FlareClusterConfiguration.DefaultBindPort
    var portRange: Int = FlareClusterConfiguration.DefaultPortRange
    var cores: Int = 1

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname" | "-h") :: value :: tail =>
          bindHostname = value
          argv = tail
        case ("--port" | "-p") :: value :: tail =>
          bindPort = value.toInt
          argv = tail
        case ("--port-range") :: value :: tail =>
          portRange = value.toInt
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case value :: tail =>
          clusterUrl = value
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }

    val clusterConf = FlareClusterConfiguration.fromUrl(clusterUrl, bindHostname, bindPort, portRange)

    try {
      run(executorId, cores, clusterConf)
    } finally {
      if (cluster != null) {
        logInfo("Leaving cluster")
        cluster.close()
      }
    }
  }
  
  private def printUsageAndExit() = {
    System.exit(1)
  }
}