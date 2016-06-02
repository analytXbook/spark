package org.apache.spark.executor.flare

import org.apache.spark.Logging
import org.apache.spark.flare.{DriverJoined, FlareCluster}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.flare.FlareMessages._
import org.apache.spark.scheduler.flare.FlareSchedulerBackend

import scala.util.{Failure, Success}

private[spark] class FlareSchedulerProxy(
    cluster: FlareCluster,
    override val rpcEnv: RpcEnv)
  extends FlareDriverProxyEndpoint(FlareSchedulerBackend.ENDPOINT_NAME, cluster) with Logging {

  var executorRef: RpcEndpointRef = _
  var registerMsg: RegisterExecutor = _

  override def receive: PartialFunction[Any, Unit] = {
    case _statusUpdate @ StatusUpdate(executorId, taskId, state, data) => {
      driverRefs.get(driverId(taskId)).map(_.send(_statusUpdate))
    }
    case reservation: FlareReservation => {
      executorRef.send(reservation)
    }
  }

  private def registerWithDriver(driverId: Int, driverRef: RpcEndpointRef) = {
    driverRef.ask[RegisteredExecutorResponse](registerMsg) onComplete {
      case Success(response) => {
        response match {
          case RegisteredExecutor => logInfo(s"Successfully registered with driver $driverId")
          case RegisterExecutorFailed(msg) => logError(s"Error registering with driver $driverId: $msg")
        }
      }
      case Failure(error) => {
        logError(s"Error registering with driver $driverId: $error")
      }
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _registerExecutor @ RegisterExecutor(executorId, _executorRef, cores, logUrls) => {
      executorRef = _executorRef
      registerMsg = RegisterExecutor(executorId, self, cores, logUrls)
      context.reply(RegisteredExecutor)
      driverRefs.foreach {
        case (driverId, driverRef) => {
          registerWithDriver(driverId, driverRef)
        }
      }
    }
    case _redeemReservation @ RedeemReservation(reservationId, _, _) => {
      pipe(_redeemReservation, driverRefs(reservationId.driverId), context)
    }
  }

  override def onDriverJoined(driver: DriverJoined) = {
    super.onDriverJoined(driver)

    registerWithDriver(driver.driverId, driverRefs(driver.driverId))
  }

}