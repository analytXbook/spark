package org.apache.spark.flare

private[spark] trait FlareClusterListener {
  def onExecutorJoined(data: ExecutorData): Unit = {}
  def onExecutorExited(data: ExecutorData): Unit = {}
  def onExecutorUpdated(data: ExecutorData): Unit = {}
  def onDriverJoined(data: DriverData): Unit = {}
  def onDriverExited(data: DriverData): Unit = {}
  def onDriverUpdated(data: DriverData): Unit = {}
  def onNodeJoined(data: NodeData): Unit = {}
  def onNodeExited(data: NodeData): Unit = {}
  def onNodeUpdated(data: NodeData): Unit = {}
}