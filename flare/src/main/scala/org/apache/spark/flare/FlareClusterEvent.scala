package org.apache.spark.flare

private[spark] sealed trait FlareClusterEvent extends Serializable

private[spark] case class NodeJoined(executorCount: Int) extends FlareClusterEvent
private[spark] case object NodeExited extends FlareClusterEvent
private[spark] case class ExecutorLaunched(executorId: String, hostname: String, port: Int) extends FlareClusterEvent
private[spark] case class FlareExecutorLost(executorId: String, time: Long, reason: String) extends FlareClusterEvent
private[spark] case class DriverJoined(driverId: Int, hostname: String, port: Int) extends FlareClusterEvent
private[spark] case class DriverExited(driverId: Int) extends FlareClusterEvent
private[spark] case class Initialize(appId: String, properties: Map[String, String]) extends FlareClusterEvent
