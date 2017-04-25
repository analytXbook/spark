package org.apache.spark.executor.flare

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.flare.FlareCluster
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.AskPermissionToCommitOutput

class FlareOutputCommitCoordinatorProxy(
    cluster: FlareCluster,
    idBackend: FlareIdBackend,
    override val rpcEnv: RpcEnv)
  extends FlareDriverProxyEndpoint("OutputCommitCoordinator", cluster, idBackend) with Logging {
  
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _askPermissionToCommitOutput @ AskPermissionToCommitOutput(stageId, partition, attemptNumber) => {
      driverRef(stageId, "stage").foreach(pipe(_askPermissionToCommitOutput, _, context))
    }
      
    case unhandled => {
      logError(s"Unhandled output committer message: $unhandled")
      context.sendFailure(new SparkException(s"Message type ${unhandled.getClass} is unhandled by spark proxy"))
    }
  }
}