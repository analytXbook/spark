package org.apache.spark.executor.flare

import org.apache.spark.flare.FlareCluster
import org.apache.spark.rpc.{RpcCallContext, RpcEnv}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.storage._
import org.apache.spark.{Logging, SparkException}

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
  
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _registerBlockManager @ RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) => {
      context.reply(true)
      
      driverRefs.foreach {
        case (driverId, rpcRef) => rpcRef.ask[Boolean](_registerBlockManager) onComplete {
          case Success(registered) => 
            if (registered) 
              logInfo(s"Registered block manager with driver $driverId")
            else
              logError(s"Unable to register block manager with driver $driverId")
          case Failure(error) => 
            logError("Failed to register block manager", error)
        }
      }
    }
    
    case _updateBlockInfo @ UpdateBlockInfo(
      blockManagerId, blockId, storageLevel, deserializedSize, size, externalBlockStoreSize) => {
      
      driverRefs.get(driverIdFromBlockId(blockId)).map(driverRef => pipe(_updateBlockInfo, driverRef, context))        
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
        case Failure(error) => logError("Failed to block locations", error)
      }
    }
    
    case _getPeers @ GetPeers(blockManagerId) => {
      pipe(_getPeers, driverRefs.head._2, context)
    }

    case msg => logInfo(s"Unhandled Block Manager master message: $msg")
  }
  
}