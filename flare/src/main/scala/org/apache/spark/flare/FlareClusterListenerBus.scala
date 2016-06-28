package org.apache.spark.flare

import org.apache.spark.util.AsynchronousListenerBus

private[spark] class FlareClusterListenerBus extends AsynchronousListenerBus[FlareClusterListener, FlareClusterEvent]("FlareClusterListenerBus") {
  override def onPostEvent(listener: FlareClusterListener, event: FlareClusterEvent): Unit = {
    event match {
      case FlareExecutorExited(data) => listener.onExecutorExited(data)
      case FlareExecutorJoined(data) => listener.onExecutorJoined(data)
      case FlareExecutorUpdated(data) => listener.onExecutorUpdated(data)
      case FlareDriverExited(data) => listener.onDriverExited(data)
      case FlareDriverJoined(data) => listener.onDriverJoined(data)
      case FlareDriverUpdated(data) => listener.onDriverUpdated(data)
      case FlareNodeExited(data) => listener.onNodeExited(data)
      case FlareNodeJoined(data) => listener.onNodeJoined(data)
      case FlareNodeUpdated(data) => listener.onNodeUpdated(data)
    }
  }

  override def onDropEvent(event: FlareClusterEvent): Unit = {}
}