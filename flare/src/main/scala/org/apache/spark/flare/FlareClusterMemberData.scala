package org.apache.spark.flare

sealed trait FlareClusterMemberData {
  def toPath: String
}
case class DriverData(driverId: Int, hostname: String, port: Int) extends FlareClusterMemberData {
  def toPath: String = s"/driver/$driverId"
}
case class ExecutorData(executorId: String, hostname: String) extends FlareClusterMemberData {
  def toPath: String = s"/executor/$executorId"
}
case class NodeData(nodeId: String, hostname: String) extends FlareClusterMemberData {
  def toPath: String = s"/node/$nodeId"
}