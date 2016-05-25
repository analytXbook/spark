package org.apache.spark.flare

trait FlareClusterListener {
  def onExecutorLaunched(executorLaunched: ExecutorLaunched): Unit = {}
  def onExecutorLost(executorLost: FlareExecutorLost): Unit = {}
  def onDriverJoined(driverJoined: DriverJoined): Unit = {}
  def onDriverExited(driverExited: DriverExited): Unit = {}
  def onNodeJoined(nodeJoined: NodeJoined): Unit = {}
  def onNodeExited: Unit = {}
  def onInitialize(initialize: Initialize): Unit = {}
}