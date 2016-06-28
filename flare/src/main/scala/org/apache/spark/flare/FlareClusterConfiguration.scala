package org.apache.spark.flare

import org.apache.spark.SparkConf

case class FlareClusterConfiguration(clusterUrl: String, hostname: String, sparkConf: SparkConf) {
  def zkUrl = clusterUrl.replace("flare://", "")
}

object FlareClusterConfiguration {
  val DEFAULT_HOSTNAME = "localhost"

  def fromUrl(
    url: String,
    hostname: String = DEFAULT_HOSTNAME,
    conf: SparkConf = new SparkConf) = FlareClusterConfiguration(url, hostname, new SparkConf())
}
