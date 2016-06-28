package org.apache.spark.flare

sealed trait FlareClusterEvent extends Serializable
case class FlareNodeJoined(data: NodeData) extends FlareClusterEvent
case class FlareNodeExited(data: NodeData) extends FlareClusterEvent
case class FlareNodeUpdated(data: NodeData) extends FlareClusterEvent
case class FlareExecutorJoined(data: ExecutorData) extends FlareClusterEvent
case class FlareExecutorExited(data: ExecutorData) extends FlareClusterEvent
case class FlareExecutorUpdated(data: ExecutorData) extends FlareClusterEvent
case class FlareDriverJoined(data: DriverData) extends FlareClusterEvent
case class FlareDriverExited(data: DriverData) extends FlareClusterEvent
case class FlareDriverUpdated(data: DriverData) extends FlareClusterEvent
