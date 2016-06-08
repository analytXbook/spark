package org.apache.spark.scheduler.flare

import org.apache.spark.TaskState.TaskState
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.SerializableBuffer
import java.nio.ByteBuffer

private[spark] case class FlareReservationId(stageId: Int, attemptId: Int, driverId: Int)
private[spark] case class FlareReservationGroupDescription(name: String, minShare: Option[Int], maxShare: Option[Int], weight: Option[Int])

private[spark] sealed trait FlareMessage extends Serializable

private[spark] object FlareMessages {
  case class FlareReservation(reservationId: FlareReservationId, count: Int, driverEndpoint: RpcEndpointRef, groups: Seq[FlareReservationGroupDescription]) extends FlareMessage

  case class RegisterExecutor(executorId: String, executorRef: RpcEndpointRef, cores: Int, logUrls: Map[String, String])
  sealed trait RegisteredExecutorResponse extends FlareMessage
  case object RegisteredExecutor extends RegisteredExecutorResponse
  case class RegisterExecutorFailed(msg: String) extends RegisteredExecutorResponse

  case class RedeemReservation(reservationId: FlareReservationId, executorId: String, host: String) extends FlareMessage

  sealed trait RedeemReservationResponse extends FlareMessage
  case class LaunchTaskReservationResponse(data: SerializableBuffer) extends RedeemReservationResponse
  case object SkipReservationResponse extends RedeemReservationResponse
  case class ThrottleReservationsResponse(delay: Long) extends RedeemReservationResponse
  case class CancelReservation(requestId: String) extends FlareMessage

  case class KillTask(taskId: Int, executorId: String, interruptThread: Boolean) extends FlareMessage

  case class StatusUpdate(
      executorId: String,
      taskId: Long,
      state: TaskState,
      data: SerializableBuffer)
    extends FlareMessage
    
  object StatusUpdate {
    def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer): StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
    }
  }
}