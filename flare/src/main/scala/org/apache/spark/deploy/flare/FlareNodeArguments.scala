package org.apache.spark.deploy.flare

import org.apache.spark.flare.FlareClusterConfiguration

private[spark] class FlareNodeArguments(args: Array[String]) {
  var clusterUrl: String = null
  var workDir: String = null
  var hostname: String = FlareClusterConfiguration.DEFAULT_HOSTNAME

  if (System.getenv("SPARK_FLARE_HOSTNAME") != null) {
    hostname = System.getenv("SPARK_FLARE_HOSTNAME")
  }

  if (System.getenv("SPARK_FLARE_CLUSTER_URL") != null) {
    clusterUrl = System.getenv("SPARK_FLARE_CLUSTER_URL")
  }

  if (System.getenv("SPARK_FLARE_WORK_DIR") != null) {
    workDir = System.getenv("SPARK_FLARE_WORK_DIR")
  }

  parse(args.toList)

  def clusterConf = FlareClusterConfiguration.fromUrl(clusterUrl, hostname)

  private def parse(args: List[String]): Unit = args match {
    case ("--hostname" | "-h") :: value :: tail =>
      hostname = value
      parse(tail)
    case ("--work-dir" | "-d") :: value :: tail =>
      workDir = value
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