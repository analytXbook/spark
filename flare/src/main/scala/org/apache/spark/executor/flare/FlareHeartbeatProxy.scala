package org.apache.spark.executor.flare

import org.apache.spark._
import org.apache.spark.flare._
import org.apache.spark.rpc._
import org.apache.spark.internal.Logging
import org.apache.spark.util.AccumulatorV2

import scala.util.{Failure, Success}

private[spark] class FlareHeartbeatProxy(
    cluster: FlareCluster,
    override val rpcEnv: RpcEnv)
  extends FlareDriverProxyEndpoint(HeartbeatReceiver.ENDPOINT_NAME, cluster) with Logging {
  
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Heartbeat(executorId, taskMetrics, blockManagerId) => {
      context.reply(HeartbeatResponse(false))
      
      val driverTaskMetrics = taskMetrics.groupBy ({
        case (taskId, metrics) =>
          //deserializing an accumulator at this proxy endpoint sets the atDriverSide flag to true.
          //before it can be forwarded to the driver, the flag needs to be set to false so that
          //it doesn't check to see if the accumulator is registered while serializing out.
          metrics.foreach( _.atDriverSide = false )
          driverId(taskId)
      }).withDefaultValue(Array[(Long, Seq[AccumulatorV2[_, _]])]())
      
      driverRefs.foreach {
        case (driverId, rpcRef) => {
          rpcRef.ask[HeartbeatResponse](Heartbeat(executorId, driverTaskMetrics(driverId), blockManagerId)) onComplete {
            case Success(HeartbeatResponse(reregisterBlockManager)) => 
            case Failure(error) => logError(s"Error heartbeating: $error", error)
          }
        }
      }
    }
  }
}