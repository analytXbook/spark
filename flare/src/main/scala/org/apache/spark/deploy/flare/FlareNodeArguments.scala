package org.apache.spark.deploy.flare

import org.apache.spark.flare.FlareClusterConfiguration

class FlareNodeArguments(args: Array[String]) {
  var clusterUrl: String = null
  var workDir: String = null
  var bindHostname: String = FlareClusterConfiguration.DefaultBindHostname
  var bindPort: Int = FlareClusterConfiguration.DefaultBindPort
  var portRange: Int = FlareClusterConfiguration.DefaultPortRange

  if (System.getenv("SPARK_FLARE_HOSTNAME") != null) {
    bindHostname = System.getenv("SPARK_FLARE_HOSTNAME")
  }

  if (System.getenv("SPARK_FLARE_PORT") != null) {
    bindPort = System.getenv("SPARK_FLARE_PORT").toInt
  }
   
  if (System.getenv("SPARK_FLARE_CLUSTER_URL") != null) {
    clusterUrl = System.getenv("SPARK_FLARE_CLUSTER_URL")
  }

  if (System.getenv("SPARK_FLARE_WORK_DIR") != null) {
    workDir = System.getenv("SPARK_FLARE_WORK_DIR")
  }

  if (System.getenv("SPARK_FLARE_PORT_RANGE") != null) {
    portRange = System.getenv("SPARK_FLARE_PORT_RANGE").toInt
  }

  parse(args.toList)

  def clusterConf = FlareClusterConfiguration.fromUrl(
    clusterUrl, bindHostname, bindPort, portRange)

  private def parse(args: List[String]): Unit = args match {
    case ("--hostname" | "-h") :: value :: tail =>
      bindHostname = value
      parse(tail)
    case ("--port" | "-p") :: value :: tail =>
      bindPort = value.toInt
      parse(tail)
    case ("--work-dir" | "-d") :: value :: tail =>
      workDir = value
      parse(tail)
    case ("--port-range") :: value :: tail =>
      portRange = value.toInt
      parse(tail)
    case value :: tail => 
      if (clusterUrl != null) {
        printUsageAndExit(1)
      }
      clusterUrl = value
      parse(tail)
    case Nil =>
      if (clusterUrl == null) {
        printUsageAndExit(1)
      }
    case _ => 
      printUsageAndExit(1)
  }
  
  private def printUsageAndExit(exitCode: Int) = {
    System.exit(exitCode)
  }

}