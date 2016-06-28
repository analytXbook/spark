package org.apache.spark.scheduler.flare

import org.apache.spark.{Logging, SparkEnv, SparkException}
import org.apache.spark.flare.{FlareCluster, _}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.flare.FlareMessages._
import org.apache.spark.util.{AkkaUtils, EncodedId, SerializableBuffer}

import scala.collection.mutable

private[spark] class FlareSchedulerBackend(scheduler: FlareScheduler, flareUrl: String)
  extends SchedulerBackend with FlareClusterListener with Logging {

  scheduler.initialize(this)

  val sc = scheduler.sc

  val conf = sc.conf
  val rpcEnv = sc.env.rpcEnv

  val clusterConf = FlareClusterConfiguration.fromUrl(flareUrl, rpcEnv.address.host, sc.conf)
  val cluster = FlareCluster(clusterConf)
  
  val listenerBus = sc.listenerBus

  val executorsPendingLossReason = new mutable.HashSet[String]
  val executors = new mutable.HashMap[String, FlareExecutorInfo]
  
  val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)

  var driverId: Int = _

  val minExecutors = conf.getInt("spark.flare.minExecutors", 1)

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", 1)
  }

  override def isReady(): Boolean = {
    executors.size >= minExecutors
  }

  override def applicationId(): String = {
    cluster.appId
  }
  
  def placeReservations(
    stageId: Int,
    stageAttemptId: Int,
    executorReservationCount: Map[String, Int],
    reservationGroups: Seq[FlareReservationGroupDescription]) = {
    executorReservationCount.foreach {
      case (executorId, reservationCount) =>
        val executor = executors(executorId)
        executor.executorEndpoint.send(
          FlareReservation(
            FlareReservationId(stageId, stageAttemptId, driverId),
            reservationCount,
            driverEndpoint,
            reservationGroups))
    }
  }

  def cancelReservations(
    stageId: Int,
    stageAttemptId: Int,
    executorIds: Seq[String]) = {
    executorIds.foreach {
      executorId => {
        val executor = executors(executorId)
        executor.executorEndpoint.send(
          CancelReservation(FlareReservationId(stageId, stageAttemptId, driverId))
        )
      }
    }
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) = {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }
 
  private class DriverEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {
        
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    override protected def log = FlareSchedulerBackend.this.log
    
    override def receive: PartialFunction[Any, Unit] = {
      case StatusUpdate(executorId, taskId, state, data) => 
        scheduler.statusUpdate(taskId, state, data.value)

      case killTaskMsg @ KillTask(taskId, executorId, interruptThread) => {
        executors.get(executorId) match {
          case Some(executorInfo) => 
            executorInfo.executorEndpoint.send(killTaskMsg)
          case None => 
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }
      }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorRef, cores, logUrls) => {
        if (executors.contains(executorId)) {
          context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        } else {
          val executorAddress = context.senderAddress
          val executor = new FlareExecutorInfo(executorRef, executorAddress.host, cores, logUrls)
          executors(executorId) = executor

          scheduler.executorRegistered(executorId, executorAddress.host)

          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")

          context.reply(RegisteredExecutor)

          listenerBus.post(SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, executor))
        }
      }

      case RedeemReservation(FlareReservationId(stageId, attemptId, _), executorId, host) => {
        scheduler.redeemReservation(stageId, attemptId, executorId, host) match {
          case Some(task) => {
            val serializedTask = ser.serialize(task)
            /*
            if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
              scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
                try {
                  var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                    "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
                    "spark.akka.frameSize or using broadcast variables for large values."
                  msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
                    AkkaUtils.reservedSizeBytes)
                  taskSetMgr.abort(msg)
                } catch {
                  case e: Exception => logError("Exception in error callback", e)
                }
              }
            } */
            context.reply(LaunchTaskReservationResponse(new SerializableBuffer(serializedTask)))
          }
          case None => {
            context.reply(SkipReservationResponse)
          }
        }
      }
    }
  }
  
  var driverEndpoint: RpcEndpointRef = null
  protected def createDriverEndpoint(): ThreadSafeRpcEndpoint = {
    return new DriverEndpoint(rpcEnv)
  }

  override def onExecutorExited(data: ExecutorData): Unit = {
    listenerBus.post(SparkListenerExecutorRemoved(System.currentTimeMillis(), data.executorId, "Executor disconnected"))
  }
  
  override def start() {
    val properties = {
      val builder = Map.newBuilder[String, String]
      for ((key, value) <- conf.getAll) {
        if (key.startsWith("spark."))
          builder += (key -> value)
      }
      builder.result
    }

    cluster.start(DriverClusterProfile(rpcEnv.address.host, rpcEnv.address.port, super.applicationId, properties))
    cluster.addListener(this)

    driverEndpoint = rpcEnv.setupEndpoint(FlareSchedulerBackend.ENDPOINT_NAME, createDriverEndpoint())
    
    driverId = cluster.localData match {
      case Some(DriverData(driverId, _, _)) => driverId
      case _ => throw new SparkException("Expected local data to be set to driver data")
    }

    EncodedId.enable(driverId)
  }
  
  override def stop() {
    cluster.close()
  }
  
  override def reviveOffers() = {}
}

private[spark] object FlareSchedulerBackend {
  val ENDPOINT_NAME = "FlareScheduler"
}