package org.apache.spark.executor.flare

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.flare._
import org.apache.spark.rpc._

import scala.util.{Failure, Success}

private[spark] class FlareHeartbeatProxy(
    cluster: FlareCluster,
    override val rpcEnv: RpcEnv)
  extends FlareDriverProxyEndpoint(HeartbeatReceiver.ENDPOINT_NAME, cluster) with Logging {
  
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Heartbeat(executorId, taskMetrics, blockManagerId) => {
      context.reply(HeartbeatResponse(false))
      
      val driverTaskMetrics = taskMetrics.groupBy ({
        case (taskId, metrics) => driverId(taskId)
      }).withDefaultValue(Array[(Long, TaskMetrics)]())
      
      driverRefs.foreach {
        case (driverId, rpcRef) => {
          rpcRef.ask[HeartbeatResponse](Heartbeat(executorId, driverTaskMetrics(driverId), blockManagerId)) onComplete {
            case Success(HeartbeatResponse(reregisterBlockManager)) => 
            case Failure(error) => logError(s"Error heartbeating: $error")
          }
        }
      }
    }
  }
}