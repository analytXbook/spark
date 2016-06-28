package org.apache.spark.flare

import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.spark.serializer.JavaSerializer

import scala.collection.JavaConversions._
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.{Logging, SparkException}
import org.apache.zookeeper.CreateMode
import org.apache.spark.util.SerializerUtils._

import scala.reflect.ClassTag

class FlareCluster(conf: FlareClusterConfiguration) extends Logging{
  private val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000
  private val RETRY_WAIT_MILLIS = 5000
  private val MAX_RECONNECT_ATTEMPTS = 3

  val client = CuratorFrameworkFactory.builder()
    .connectString(conf.zkUrl)
    .sessionTimeoutMs(ZK_SESSION_TIMEOUT_MILLIS)
    .connectionTimeoutMs(ZK_CONNECTION_TIMEOUT_MILLIS)
    .retryPolicy(new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    .namespace("flare")
    .build()

  val eventBus: FlareClusterListenerBus = new FlareClusterListenerBus

  val serializer: JavaSerializer = new JavaSerializer(conf.sparkConf)

  val counterService = new FlareCounterService(client)

  class CacheListenerBroadcaster[T <: FlareClusterMemberData : ClassTag](
    addedEvent: T => FlareClusterEvent,
    removedEvent: T => FlareClusterEvent,
    updatedEvent: T => FlareClusterEvent) extends PathChildrenCacheListener {
    import PathChildrenCacheEvent.Type._
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      def ser = serializer.newInstance()
      val data = ser.fromBytes[T](event.getData.getData)
      event.getType match {
        case CHILD_ADDED => eventBus.post(addedEvent(data))
        case CHILD_REMOVED => eventBus.post(removedEvent(data))
        case CHILD_UPDATED => eventBus.post(updatedEvent(data))
      }
    }
  }

  private val driverCache = new PathChildrenCache(client, "/driver", true)
  driverCache.getListenable.addListener(
    new CacheListenerBroadcaster[DriverData](FlareDriverJoined.apply, FlareDriverExited.apply, FlareDriverUpdated.apply))

  private val nodeCache = new PathChildrenCache(client, "/node", true)
  nodeCache.getListenable.addListener(
    new CacheListenerBroadcaster[NodeData](FlareNodeJoined.apply, FlareNodeExited.apply, FlareNodeUpdated.apply))

  private val executorCache = new PathChildrenCache(client, "/executor", true)
  executorCache.getListenable.addListener(
    new CacheListenerBroadcaster[ExecutorData](FlareExecutorJoined.apply, FlareExecutorExited.apply, FlareExecutorUpdated.apply))

  private var localMemberData: Option[FlareClusterMemberData] = None

  private var localProfile: Option[FlareClusterProfile] = None

  def start(profile: FlareClusterProfile): Unit = {
    localProfile = Some(profile)
    client.start()
    driverCache.start(StartMode.BUILD_INITIAL_CACHE)
    nodeCache.start(StartMode.BUILD_INITIAL_CACHE)
    executorCache.start(StartMode.BUILD_INITIAL_CACHE)
    counterService.start()
    profile.start(this)
    eventBus.start(null)
  }

  def close() = {
    driverCache.close()
    nodeCache.close()
    executorCache.close()
    client.close()
    eventBus.stop()
  }

  def reset() = {
    localProfile.getOrElse(throw new SparkException("Attempting to reset cluster that has not be started")).reset(this)
  }

  def drivers: Seq[DriverData] = {
    val ser = serializer.newInstance
    driverCache.getCurrentData.map {
      childData => ser.fromBytes[DriverData](childData.getData)
    }
  }

  def executors: Seq[ExecutorData] = {
    val ser = serializer.newInstance
    executorCache.getCurrentData.map {
      childData => ser.fromBytes[ExecutorData](childData.getData)
    }
  }

  def nodes: Seq[NodeData] = {
    val ser = serializer.newInstance
    nodeCache.getCurrentData.map {
      childData => ser.fromBytes[NodeData](childData.getData)
    }
  }

  def appId: String = {
    val ser = serializer.newInstance
    ser.fromBytes[String](client.getData.forPath("/init/appId"))
  }

  def properties: Map[String, String] = {
    val ser = serializer.newInstance
    ser.fromBytes[Map[String, String]](client.getData.forPath("/init/properties"))
  }

  def addListener(listener: FlareClusterListener): Unit = {
    eventBus.addListener(listener)
  }

  def counter(name: String, initialValue: Long = 0l): FlareCounter = {
    counterService.create(name, initialValue)
  }

  private[flare] def register(data: FlareClusterMemberData) = {
    if (localMemberData.isDefined) {
      throw new SparkException("Attempting to register after already being registered")
    }
    localMemberData = Some(data)
    val ser = serializer.newInstance
    client.create().withMode(CreateMode.EPHEMERAL).forPath(data.toPath, ser.toBytes(data))
  }

  private[spark] def localData = localMemberData
}

object FlareCluster {
  def apply(conf: FlareClusterConfiguration) = new FlareCluster(conf)
}