package org.apache.spark.flare

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.util.Utils

import scala.collection.convert.decorateAsScala._
import org.jgroups.Address
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkException


class FlareClusterState extends Serializable {
  val nodes = new ConcurrentHashMap[Address, FlareNodeInfo]().asScala
  val drivers = new ConcurrentHashMap[Int, FlareDriverInfo]().asScala
  val driverAddress = new ConcurrentHashMap[Address, Int]().asScala
  val executors = new ConcurrentHashMap[String, FlareExecutorInfo]().asScala

  private var _appId: Option[String] = None
  private var _properties: Option[Map[String, String]] = None

  def properties = _properties.getOrElse(throw new SparkException("Attempted to get properties of unitialized flare cluster"))

  def appId: String = _appId.getOrElse(throw new Exception("Attempted to get appId of uninitialized cluster"))

  def initialize(appId: String, properties: Map[String, String]) = {
    _appId = Some(appId)
    _properties = Some(properties)
  }

  def uninitialize() = {
    _appId = None
    _properties = None
  }

  def isInitialized = _properties.isDefined

  def clear() = {
    _properties = None
    _appId = None
    nodes.clear()
    drivers.clear()
    driverAddress.clear()
    executors.clear()
  }
  
  def load(state: FlareClusterState) = {
    clear()

    nodes ++= state.nodes
    drivers ++= state.drivers
    driverAddress ++= state.driverAddress
    executors ++= state.executors
    _properties = state._properties
    _appId = state._appId
  }
}

case class FlareNodeInfo(executorCount: Int)
case class FlareDriverInfo(hostname: String, port: Int)
case class FlareExecutorInfo(hostname: String, port: Int)