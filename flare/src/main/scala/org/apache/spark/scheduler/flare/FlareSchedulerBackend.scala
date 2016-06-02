package org.apache.spark.scheduler.flare

import org.apache.spark.{Logging, SparkEnv}
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

  val cluster = FlareCluster(flareUrl, conf)
  
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
    cluster.state.appId
  }
  
  def placeReservations(stageId: Int, stageAttemptId: Int, executorReservationCount: Map[String, Int]) = {
    executorReservationCount.foreach {
      case (executorId, reservationCount) =>
        val executor = executors(executorId)
        executor.executorEndpoint.send(
          FlareReservation(
            FlareReservationId(stageId, stageAttemptId, driverId),
            reservationCount,
            driverEndpoint,
            Seq.empty))
    }
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
        if (scheduler.isMaxParallelism(stageId, attemptId)) {
          context.reply(ThrottleReservationsResponse(100))
        } 
        else {
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
  }
  
  var driverEndpoint: RpcEndpointRef = null
  protected def createDriverEndpoint(): ThreadSafeRpcEndpoint = {
    return new DriverEndpoint(rpcEnv)
  }

  override def onExecutorLost(executorLost: FlareExecutorLost): Unit = {
    listenerBus.post(SparkListenerExecutorRemoved(executorLost.time, executorLost.executorId, executorLost.reason))
  }
  
  override def start() {
    cluster.connect()
    cluster.addListener(this)

    val properties = {
      val builder = Map.newBuilder[String, String]
      for ((key, value) <- conf.getAll) {
        if (key.startsWith("spark."))
          builder += (key -> value)
      }
      builder.result
    }

    if (cluster.state.isInitialized) {
      //val existingProps = cluster.state.properties
      //if (!properties.equals(existingProps))
        //throw new SparkException("Attempting to connect to flare cluster with different app properties")
    } else {
      cluster.send(Initialize(super.applicationId, properties))
    }

    driverEndpoint = rpcEnv.setupEndpoint(FlareSchedulerBackend.ENDPOINT_NAME, createDriverEndpoint())
    
    driverId = cluster.counter("driverId").incrementAndGet().toInt

    EncodedId.enable(driverId)
        
    cluster.send(DriverJoined(driverId, driverEndpoint.address.host, driverEndpoint.address.port))
  }
  
  override def stop() {
    cluster.close()
  }
  
  override def reviveOffers() = {}
}

private[spark] object FlareSchedulerBackend {
  val ENDPOINT_NAME = "FlareScheduler"
}