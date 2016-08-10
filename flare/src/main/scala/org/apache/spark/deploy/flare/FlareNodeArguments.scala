package org.apache.spark.deploy.flare

import org.apache.spark.flare.FlareClusterConfiguration
import org.apache.spark.util.Utils

private[spark] class FlareNodeArguments(args: Array[String]) {
  var clusterUrl: String = null
  var workDir: String = null
  var hostname: String = FlareClusterConfiguration.DEFAULT_HOSTNAME
  var executorCount: Int = 1
  var executorCores: Int = 1
  var executorMemory: String = "1g"
  var redisHost: String = "localhost"

  def executorMemoryAsMb = Utils.byteStringAsMb(executorMemory)

  if (System.getenv("SPARK_FLARE_HOSTNAME") != null) {
    hostname = System.getenv("SPARK_FLARE_HOSTNAME")
  }

  if (System.getenv("SPARK_FLARE_CLUSTER_URL") != null) {
    clusterUrl = System.getenv("SPARK_FLARE_CLUSTER_URL")
  }

  if (System.getenv("SPARK_FLARE_WORK_DIR") != null) {
    workDir = System.getenv("SPARK_FLARE_WORK_DIR")
  }

  if (System.getenv("SPARK_FLARE_EXECUTOR_COUNT") != null) {
    executorCount = System.getenv("SPARK_FLARE_EXECUTOR_COUNT").toInt
  }

  if (System.getenv("SPARK_FLARE_EXECUTOR_CORES") != null) {
    executorCores = System.getenv("SPARK_FLARE_EXECUTOR_CORES").toInt
  }

  if (System.getenv("SPARK_FLARE_EXECUTOR_MEMORY") != null){
    executorMemory = System.getenv("SPARK_FLARE_EXECUTOR_MEMORY")
  }

  if (System.getenv("SPARK_FLARE_REDIS_HOST") != null){
    redisHost = System.getenv("SPARK_FLARE_REDIS_HOST")
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
    case ("--executors" | "-e") :: value :: tail =>
      executorCount = value.toInt
      parse(tail)
    case ("--executor-cores" | "-c") :: value :: tail =>
      executorCores = value.toInt
      parse(tail)
    case ("--executor-memory" | "-m") :: value :: tail =>
      executorMemory = value
      parse(tail)
    case "--redis-host" :: value :: tail =>
      redisHost = value
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