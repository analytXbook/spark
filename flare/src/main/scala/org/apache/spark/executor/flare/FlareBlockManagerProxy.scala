package org.apache.spark.executor.flare

import org.apache.spark.flare.{DriverData, FlareCluster}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.storage._
import org.apache.spark.internal.Logging
import org.apache.spark.SparkException

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

private[spark] class FlareBlockManagerProxy(
    cluster: FlareCluster,
    override val rpcEnv: RpcEnv)
  extends FlareDriverProxyEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME, cluster) with Logging {
  
  def driverIdFromBlockId(blockId: BlockId): Int = {
    blockId match {
      case RDDBlockId(rddId, _) => driverId(rddId)
      case ShuffleBlockId(shuffleId, _, _) => driverId(shuffleId)
      case ShuffleDataBlockId(shuffleId, _, _) => driverId(shuffleId)
      case ShuffleIndexBlockId(shuffleId, _, _) => driverId(shuffleId)
      case BroadcastBlockId(broadcastId, _) => driverId(broadcastId)
      case TaskResultBlockId(taskId) => driverId(taskId)
      case StreamBlockId(streamId, _) => driverId(streamId)
      case _ => throw new SparkException(s"Unknown blockId type: $blockId")
    }
  }

  var capturedRegistration: Option[RegisterBlockManager] = None
  val registeredDrivers = mutable.Set.empty[Int]

  private def registerWithDriver(driverId: Int, driverRef: RpcEndpointRef): Future[Boolean] = {
    logInfo(s"Registering with driver $driverId block manager")

    val future = capturedRegistration match {
      case Some(slaveRegistrationMsg) => driverRef.ask[Boolean](slaveRegistrationMsg)
      case None => Future.failed(new SparkException("Attempted to register with driver before receiving RegisterBlockManager message from executor"))
    }

    future.onComplete {
      case Success(registered) =>
        if (registered) {
          registeredDrivers.add(driverId)
          logInfo(s"Registered block manager with driver $driverId")
        }
        else
          logError(s"Unable to register block manager with driver $driverId")
      case Failure(error) =>
        logError(s"Failed to register block manager with driver $driverId", error)
    }

    future
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _registerBlockManager @ RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) => {
      if (!capturedRegistration.isDefined) {
        logInfo(s"Initializing FlareBlockManagerProxy")
        capturedRegistration = Some(_registerBlockManager)

        logInfo(s"Setting up this proxy endpoint as BlockManagerSlave endpoint: ${slaveEndpoint.name}")
        rpcEnv.setupEndpoint(slaveEndpoint.name, this)

        logInfo(s"Registering with all known drivers")
        driverRefs.foreach{ case (driverId, driverRef) => registerWithDriver(driverId, driverRef)}
      } else {
        logInfo("Already initialized and asked to register with driver")
        val pendingDrivers = driverRefs.keySet.diff(registeredDrivers)

        if (pendingDrivers.isEmpty)
          logInfo("No pending drivers to register with")
        else {
          logInfo(s"Registering with pending drivers: ${pendingDrivers.mkString(",")}")
          pendingDrivers.foreach { driverId =>
            registerWithDriver(driverId, driverRefs(driverId))
          }
        }
      }
      context.reply(true)
    }

    case _updateBlockInfo @ UpdateBlockInfo(
      blockManagerId, blockId, storageLevel, deserializedSize, size) => {
      val driverId = driverIdFromBlockId(blockId)
      driverRefs.get(driverId) match {
        case Some(driverRef) => driverRef.ask[Boolean](_updateBlockInfo) onComplete {
          case Success(reregister) => {
            if (reregister) {
              logWarning(s"Told to reregister when updating block status for ${blockId.name} with driver $driverId, no longer considered registered with driver")
              registeredDrivers.remove(driverId)
            }

            context.reply(reregister)
          }
          case Failure(error) => context.sendFailure(error)
        }
        case None => {
          val msg = s"Could not find reference to driver $driverId to update block info for ${blockId.name}"
          logError(msg)
          context.sendFailure(new SparkException(msg))
        }
      }
    }

    case _getLocations @ GetLocations(blockId) => {
      pipe(_getLocations, driverRefs(driverIdFromBlockId(blockId)), context)
    }

    case GetLocationsMultipleBlockIds(blockIds) => {
      val futures = blockIds.groupBy(driverIdFromBlockId(_)).map {
        case (driverId, blockIds) => {
          val driverRef = driverRefs(driverId)
          driverRef.ask[Seq[BlockManagerId]](GetLocationsMultipleBlockIds(blockIds)).map(blockIds.zip(_))
        }
      }
      Future.sequence(futures).map(_.flatten.toMap) onComplete {
        case Success(locations) => context.reply(blockIds.map(locations(_)))
        case Failure(error) => logError("Failed to get block locations", error)
      }
    }

    case _getPeers @ GetPeers(blockManagerId) => {
      pipe(_getPeers, driverRefs.head._2, context)
    }

    case slaveMsg: ToBlockManagerSlave => {
      val slaveRef = capturedRegistration.map(_.sender).getOrElse(
        throw new SparkException("Received message for slave without slave registered"))
      pipe(slaveMsg, slaveRef, context)
    }

    case msg => logError(s"Unhandled Block Manager message: $msg")
  }

  override def onDriverJoined(data: DriverData) = {
    super.onDriverJoined(data)

    registerWithDriver(data.driverId, driverRefs(data.driverId))
  }
  
}