package org.apache.spark.flare


import org.apache.spark.util.AsynchronousListenerBus
import org.apache.spark.flare._

private[spark] class FlareClusterListenerBus extends AsynchronousListenerBus[FlareClusterListener, FlareClusterEvent]("FlareClusterListenerBus") {
  override def onPostEvent(listener: FlareClusterListener, event: FlareClusterEvent): Unit = {
    event match {
      case executorLaunched: ExecutorLaunched => listener.onExecutorLaunched(executorLaunched)
      case executorLost: FlareExecutorLost => listener.onExecutorLost(executorLost)
      case driverJoined: DriverJoined => listener.onDriverJoined(driverJoined)
      case driverExited: DriverExited => listener.onDriverExited(driverExited)
      case nodeJoined: NodeJoined => listener.onNodeJoined(nodeJoined)
      case NodeExited => listener.onNodeExited
      case initialize: Initialize => listener.onInitialize(initialize)
      case _ => 
    }
  }
  
  override def onDropEvent(event: FlareClusterEvent): Unit = {}
}