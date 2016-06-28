package org.apache.spark.executor.flare

import org.apache.spark.flare._
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpointRef, ThreadSafeRpcEndpoint}
import org.apache.spark.util.{EncodedId, ThreadUtils}
import org.apache.spark.{Logging, SparkEnv}

import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

private[spark] abstract class FlareDriverProxyEndpoint(endpointName: String, cluster: FlareCluster) extends ThreadSafeRpcEndpoint with FlareClusterListener with Logging{
  protected implicit val ec = FlareDriverProxyEndpoint.executionContext
  
  protected val driverRefs = new HashMap[Int, RpcEndpointRef]()
  
  override def onStart = {
    cluster.addListener(this)
    
    cluster.drivers.foreach {
      case DriverData(driverId, hostname, port) => addDriver(driverId, hostname, port)
    }
  }
  
  protected def pipe(msg: Any, ref: RpcEndpointRef, context: RpcCallContext): Unit = {
    ref.ask[Any](msg) onComplete {
      case Success(response) => context.reply(response)
      case Failure(error) => context.sendFailure(error)
    }
  }
  
  private def addDriver(driverId: Int, hostname: String, port: Int) = {
    val rpcRef = rpcEnv.setupEndpointRef(SparkEnv.driverActorSystemName, RpcAddress(hostname, port), endpointName)
    driverRefs(driverId) = rpcRef
  }
  
  override def onDriverJoined(data: DriverData) = {
    addDriver(data.driverId, data.hostname, data.port)
  }
  
  override def onDriverExited(data: DriverData) = {
    driverRefs.remove(data.driverId)
  }
  
  protected def driverId(encodedId: Long) = EncodedId.decode(encodedId)._1
  protected def driverId(encodedId: Int) = EncodedId.decode(encodedId)._1
}

private[spark] object FlareDriverProxyEndpoint {
  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("flare-driver-proxy-thread-pool")
  implicit val executionContext = ExecutionContext.fromExecutorService(askThreadPool)
}