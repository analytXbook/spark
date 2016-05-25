package org.apache.spark.executor.flare

import java.net.URL
import java.nio.ByteBuffer

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
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class FlareExecutorBackend(
    executorId: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv,
    proxyRef: RpcEndpointRef)
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  case object AttemptLaunchReservation
  
  logInfo("Starting Flare Executor Backend")
  
  override val rpcEnv = env.rpcEnv

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("flare-executor-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)
  
  private val maxActiveTasks = cores
  private var activeTasks = 0

  val driverEndpoints = new mutable.HashMap[FlareReservationId, RpcEndpointRef]

  val rootPool = FlareReservationPool.createRootPool

  private def handleReservation(reservation: FlareReservation) = {
    logInfo(s"Received new reservation: $reservation")

    driverEndpoints(reservation.reservationId) = reservation.driverEndpoint
    rootPool.addReservation(reservation.reservationId, reservation.count, reservation.groups)
    
    attemptLaunchReservation()
  }
  
  private def attemptLaunchReservation(): Unit = {
    self.send(AttemptLaunchReservation)
  }
  
  private def launchReservation(reservationId: FlareReservationId): Unit = {
    activeTasks += 1
    rootPool.addRunningTask(reservationId)

    val driverEndpoint = driverEndpoints(reservationId)
    driverEndpoint.ask[RedeemReservationResponse](RedeemReservation(reservationId, executorId, hostname)) onComplete {
      case Success(response) => response match {
        case SkipReservationResponse =>
          activeTasks -= 1
          attemptLaunchReservation()
        case LaunchTaskReservationResponse(taskData) => 
          val taskDesc = ser.deserialize[TaskDescription](taskData.value)
          executor.launchTask(this, taskDesc.taskId, taskDesc.attemptNumber, taskDesc.name, taskDesc.serializedTask)
        case _ =>
      }
      case Failure(failure) => 
        activeTasks -= 1
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
      activeTasks -= 1
      attemptLaunchReservation()
    }
  }
  
  override def receive: PartialFunction[Any, Unit] = {
    case reservation: FlareReservation => handleReservation(reservation)
    case AttemptLaunchReservation => {
      if (activeTasks < maxActiveTasks) {
        val next = rootPool.nextReservation
        next.map(launchReservation(_))
        if (next.isDefined) {
          attemptLaunchReservation()
        }
      }
    }
    case _ =>
  }  
}

object FlareExecutorBackend extends Logging {
  val ENDPOINT_NAME = "Executor"
  val PROXY_PORT = 21000
  val EXECUTOR_PORT = 20000

  private def run(
    executorId: String,
    cores: Int,
    clusterConf: FlareClusterConfiguration) {
    
    SignalLogger.register(log)
    
    SparkHadoopUtil.get.runAsSparkUser { () =>
      val cluster = FlareCluster(clusterConf)
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
      
      env.rpcEnv.setupEndpoint(ENDPOINT_NAME, new FlareExecutorBackend(executorId, proxyRpcEnv.address.host, cores, userClassPath, env, driverRef))
   
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
    
    run(executorId, cores, clusterConf)
  }
  
  private def printUsageAndExit() = {
    System.exit(1)
  }
}