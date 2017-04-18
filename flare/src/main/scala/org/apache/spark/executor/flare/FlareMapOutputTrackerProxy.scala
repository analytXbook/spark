package org.apache.spark.executor.flare

import org.apache.spark.{GetMapOutputStatuses, MapOutputTracker}
import org.apache.spark.internal.Logging
import org.apache.spark.flare.FlareCluster
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}

import scala.util.{Failure, Success}

private[spark] class FlareMapOutputTrackerProxy(
    cluster: FlareCluster,
    idBackend: FlareIdBackend,
    override val rpcEnv: RpcEnv)
  extends FlareDriverProxyEndpoint(MapOutputTracker.ENDPOINT_NAME, cluster, idBackend) with Logging {
  
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case msg @ GetMapOutputStatuses(shuffleId: Int) => {
      val driverRef = driverRefs(driverId(shuffleId, "shuffle"))
      driverRef.ask[Array[Byte]](msg) onComplete {
        case Success(statuses) => context.reply(statuses)
        case Failure(error) => logError(s"Error retreiving statuses: $error")
      }
    }
  }
}