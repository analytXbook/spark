package org.apache.spark.executor.flare

import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.flare._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.flare.FlareMessages._
import org.apache.spark.scheduler.flare._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.{ShutdownHookManager, SignalLogger, ThreadUtils}

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
    proxyRpcEnv: RpcEnv,
    cluster: FlareCluster,
    redisConf: RedisFlarePoolBackendConfiguration)
  extends ThreadSafeRpcEndpoint with ExecutorBackend with FlareClusterListener with Logging {

  case object AttemptLaunchReservation
  case class RedemptionRejected(reservationId: FlareReservationId)
  case class CleanUpFinishedTask(taskId: Long)
  
  logInfo("Starting Flare Executor Backend")
  
  override val rpcEnv = env.rpcEnv

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("flare-executor-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val driverEndpoints = new mutable.HashMap[FlareReservationId, RpcEndpointRef]

  private val poolBackend = new RedisLuaFlarePoolBackend(redisConf)
  poolBackend.initialize(executorId)

  private val taskToReservationId = new HashMap[Long, FlareReservationId]
  private val reservationTasks = new HashMap[FlareReservationId, Set[Long]] with MultiMap[FlareReservationId, Long]

  private val activeTasks = new AtomicInteger()

  private val attemptLaunchScheduled = new AtomicBoolean()
  private val attemptLaunchScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("flare-executor-attempt-launch-scheduler")

  private def handleReservation(reservation: FlareReservation) = {
    logDebug(s"Received reservation: $reservation")

    driverEndpoints(reservation.reservationId) = reservation.driverEndpoint
    poolBackend.addReservation(reservation.reservationId, reservation.count, reservation.groups)
    
    attemptLaunchReservation()
  }

  private def cancelReservation(reservationId: FlareReservationId) = {
    logDebug(s"Cancelling reservation $reservationId")

    reservationTasks.get(reservationId).foreach(_.foreach(killTask(_, false)))

    poolBackend.removeReservation(reservationId)

    attemptLaunchReservation()
  }

  private def killTask(taskId: Long, interrupt: Boolean) = {
    executor.killTask(taskId, interrupt)
  }

  private def scheduleAttemptLaunchReservation(): Unit = {
    if (!attemptLaunchScheduled.get) {
      attemptLaunchScheduler.schedule(
        new Runnable {
          def run() = {
            attemptLaunchReservation()
            attemptLaunchScheduled.set(false)
          }
        }, 1000, TimeUnit.MILLISECONDS)
      attemptLaunchScheduled.set(true)
    }
  }

  private def attemptLaunchReservation(): Unit = {
    self.send(AttemptLaunchReservation)
  }

  private def redemptionRejected(reservationId: FlareReservationId) = {
    self.send(RedemptionRejected(reservationId))
  }

  private def launchReservation(reservationId: FlareReservationId): Unit = {
    activeTasks.incrementAndGet()

    val driverEndpoint = driverEndpoints(reservationId)
    driverEndpoint.ask[RedeemReservationResponse](RedeemReservation(reservationId, executorId, hostname)) onComplete {
      case Success(response) => response match {
        case SkipReservationResponse =>
          redemptionRejected(reservationId)
          attemptLaunchReservation()
        case LaunchTaskReservationResponse(taskData) => 
          val taskDesc = ser.deserialize[TaskDescription](taskData.value)
          taskToReservationId(taskDesc.taskId) = reservationId
          reservationTasks.addBinding(reservationId, taskDesc.taskId)
          executor.launchTask(this, taskDesc.taskId, taskDesc.attemptNumber, taskDesc.name, taskDesc.serializedTask)
          poolBackend.taskLaunched(reservationId)
        case _ =>
      }
      case Failure(failure) =>
        redemptionRejected(reservationId)
        attemptLaunchReservation()
    }
  }
  
  var executor: Executor = _
  
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()
  
  override def onStart() = {
    cluster.addListener(this)
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

  def shutdown(): Unit = {
    logInfo("Shutting down")
    executor.stop()
    stop()
    rpcEnv.shutdown()
    proxyRpcEnv.shutdown()
    cluster.close()
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {

  }
  
  override def statusUpdate(taskId: Long, state: TaskState.TaskState, data: ByteBuffer) = {
    val msg = StatusUpdate(executorId, taskId, state, data)
    proxyRef.send(msg)
    
    if (TaskState.isFinished(state)) {
      self.send(CleanUpFinishedTask(taskId))

      attemptLaunchReservation()
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case reservation: FlareReservation => handleReservation(reservation)
    case CancelReservation(reservationId) => cancelReservation(reservationId)
    case AttemptLaunchReservation => {
      if (activeTasks.get < cores) {
        val startTime = System.nanoTime()
        val nextReservation = poolBackend.nextReservation
        val duration = System.nanoTime() - startTime
        logDebug(s"nextReservation took $duration ns, result: $nextReservation")

        nextReservation match {
          case Some(reservationId) => {
            launchReservation(reservationId)
            attemptLaunchReservation()
          }
          case None => {
            //it is possible that no reservation comes back due to all max-shares being reached
            //schedule another launch attempt in the future to handle this case
            scheduleAttemptLaunchReservation()
          }
        }
      }
    }
    case RedemptionRejected(reservationId) => {
      activeTasks.decrementAndGet()
      poolBackend.taskRejected(reservationId)
    }
    case CleanUpFinishedTask(taskId) => {
      activeTasks.decrementAndGet()
      taskToReservationId.get(taskId) match {
        case Some(reservationId) => {
          poolBackend.taskFinished(reservationId)
          reservationTasks.removeBinding(reservationId, taskId)
          taskToReservationId.remove(taskId)
        }
        case None => {
          logWarning("Could not find reservation for taskId, may be duplicate finished status update ")
        }
      }
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
    clusterConf: FlareClusterConfiguration,
    redisConf: RedisFlarePoolBackendConfiguration) {
    
    SignalLogger.register(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      cluster = FlareCluster(clusterConf)
      cluster.start(ExecutorClusterProfile(executorId, clusterConf.hostname))

      val executorConf = clusterConf.sparkConf

      logInfo(s"Driver Properties: ${cluster.properties}")

      cluster.properties.foreach {
        case (key, value) => executorConf.set(key, value)
      }

      executorConf.set("spark.app.id", cluster.appId)

      val proxyConf = new SparkConf
      val proxyRpcEnv = RpcEnv.create("sparkDriver", clusterConf.hostname, PROXY_PORT, proxyConf, new SecurityManager(proxyConf), false)

      proxyRpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, new FlareHeartbeatProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, new FlareMapOutputTrackerProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME, new FlareBlockManagerProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint("OutputCommitCoordinator", new FlareOutputCommitCoordinatorProxy(cluster, proxyRpcEnv))
      proxyRpcEnv.setupEndpoint(FlareSchedulerBackend.ENDPOINT_NAME, new FlareSchedulerProxy(cluster, proxyRpcEnv))

      executorConf.set("spark.driver.host", proxyRpcEnv.address.host)
      executorConf.set("spark.driver.port", proxyRpcEnv.address.port.toString)

      val env = SparkEnv.createExecutorEnv(executorConf, executorId, clusterConf.hostname, EXECUTOR_PORT, cores, isLocal = false)
     
      val driverRef = env.rpcEnv.setupEndpointRef("sparkDriver", proxyRpcEnv.address, FlareSchedulerBackend.ENDPOINT_NAME)

      val userClassPath = List.empty[URL]

      val backend = new FlareExecutorBackend(executorId, proxyRpcEnv.address.host, cores, userClassPath, env, driverRef, proxyRpcEnv, cluster, redisConf)

      env.rpcEnv.setupEndpoint(ENDPOINT_NAME, backend)

      ShutdownHookManager.addShutdownHook { () =>
        log.debug("Got Shutdown hook")
        backend.shutdown()
      }

      env.rpcEnv.awaitTermination()
      proxyRpcEnv.awaitTermination()
    }
  }
  
  def main(args: Array[String]) {
    var executorId: String = null
    var clusterUrl: String = null
    var hostname: String = FlareClusterConfiguration.DEFAULT_HOSTNAME
    var cores: Int = 1
    var redisHost: String = "localhost"

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname" | "-h") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores" | "-c") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--redis-host") :: value :: tail =>
          redisHost = value
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

    val clusterConf = FlareClusterConfiguration.fromUrl(clusterUrl, hostname)
    val redisConf = RedisFlarePoolBackendConfiguration(redisHost)

    try {
      run(executorId, cores, clusterConf, redisConf)
    } finally {

    }
  }
  
  private def printUsageAndExit() = {
    System.exit(1)
  }
}