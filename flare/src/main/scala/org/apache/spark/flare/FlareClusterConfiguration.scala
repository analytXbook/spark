package org.apache.spark.flare

import org.apache.spark.{SparkConf, SparkException}

case class FlareClusterConfiguration(
  channelName: String,
  bindHostname: String,
  bindPort: Int,
  initialMembers: Seq[(String, Int)],
  portRange: Int) {

  def clusterUrl = initialMembers.map {
    case (host, port) => s"$host:$port"
  }.mkString(",") + s"/$channelName"
}

object FlareClusterConfiguration {
  val DefaultBindPort = 7800
  val DefaultBindHostname = "localhost"
  val DefaultPortRange = 10

  def parseUrl(url: String): (Seq[(String, Int)], String) = {
    url.split('/') match {
      case Array(hosts, channel) => {
        val members = hosts.split(',').map(_.split(':') match {
          case Array(host) => (host, DefaultBindPort)
          case Array(host, port) => (host, port.toInt)
        })
        (members, channel)
      }
      case _ => throw new SparkException(s"Could not parse flare url: $url")
    }
  }

  def fromUrl(
    url: String,
    bindHostName: String,
    bindPort: Int = DefaultBindPort,
    portRange: Int = DefaultPortRange) = {

    val (initialMembers, channelName) = parseUrl(url)

    FlareClusterConfiguration(channelName, bindHostName, bindPort, initialMembers, portRange)
  }

  def fromUrl(url: String, conf: SparkConf) = {
    val bindHostname = conf.get("spark.flare.bindHostname", DefaultBindHostname)
    val bindPort = conf.getInt("spark.flare.bindPort", DefaultBindPort)
    val portRange = conf.getInt("spark.flare.portRange", DefaultPortRange)

    val (initialMembers, channelName) = parseUrl(url)

    FlareClusterConfiguration(channelName, bindHostname, bindPort, initialMembers, portRange)
  }
}

