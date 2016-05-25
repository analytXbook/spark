package org.apache.spark.flare

sealed trait FlareClusterEvent extends Serializable

case class NodeJoined(executorCount: Int) extends FlareClusterEvent
case object NodeExited extends FlareClusterEvent
case class ExecutorLaunched(executorId: String, hostname: String, port: Int) extends FlareClusterEvent
case class FlareExecutorLost(executorId: String, time: Long, reason: String) extends FlareClusterEvent
case class DriverJoined(driverId: Int, hostname: String, port: Int) extends FlareClusterEvent
case class DriverExited(driverId: Int) extends FlareClusterEvent
case class Initialize(appId: String, properties: Map[String, String]) extends FlareClusterEvent
