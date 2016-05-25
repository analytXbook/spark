package org.apache.spark.executor.flare

import org.apache.spark.Logging
import org.apache.spark.flare.FlareCluster
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.AskPermissionToCommitOutput

class FlareOutputCommitCoordinatorProxy(
    cluster: FlareCluster,
    override val rpcEnv: RpcEnv)
  extends FlareDriverProxyEndpoint("OutputCommitCoordinator", cluster) with Logging {
  
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _askPermissionToCommitOutput @ AskPermissionToCommitOutput(stage, partition, attemptNumber) => {
      pipe(_askPermissionToCommitOutput, driverRefs(driverId(stage)), context)
    }
      
    case _ => 
  }
}