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

  val zk = CuratorFrameworkFactory.builder()
    .connectString(conf.zkUrl)
    .sessionTimeoutMs(ZK_SESSION_TIMEOUT_MILLIS)
    .connectionTimeoutMs(ZK_CONNECTION_TIMEOUT_MILLIS)
    .retryPolicy(new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    .namespace("flare")
    .build()

  val eventBus: FlareClusterListenerBus = new FlareClusterListenerBus

  val serializer: JavaSerializer = new JavaSerializer(conf.sparkConf)

  val counterService = new FlareCounterService(zk)

  class CacheListenerBroadcaster[T <: FlareClusterMemberData : ClassTag](
    addedEvent: T => FlareClusterEvent,
    removedEvent: T => FlareClusterEvent,
    updatedEvent: T => FlareClusterEvent) extends PathChildrenCacheListener {
    import PathChildrenCacheEvent.Type._
    override def childEvent(zk: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      def ser = serializer.newInstance()
      Option(event.getData()) foreach {childData =>
        val data = ser.fromBytes[T](childData.getData())
        event.getType match {
          case CHILD_ADDED => eventBus.post(addedEvent(data))
          case CHILD_REMOVED => eventBus.post(removedEvent(data))
          case CHILD_UPDATED => eventBus.post(updatedEvent(data))
        }
      }
    }
  }

  private var driverCache: PathChildrenCache = _
  private var nodeCache: PathChildrenCache = _
  private var executorCache: PathChildrenCache = _

  private var localMemberData: Option[FlareClusterMemberData] = None
  private var localProfile: Option[FlareClusterProfile] = None

  private def startMemberCaches() = {
    driverCache = new PathChildrenCache(zk, "/driver", true)
    driverCache.getListenable.addListener(
      new CacheListenerBroadcaster[DriverData](FlareDriverJoined.apply, FlareDriverExited.apply, FlareDriverUpdated.apply))
    driverCache.start(StartMode.BUILD_INITIAL_CACHE)

    nodeCache = new PathChildrenCache(zk, "/node", true)
    nodeCache.getListenable.addListener(
      new CacheListenerBroadcaster[NodeData](FlareNodeJoined.apply, FlareNodeExited.apply, FlareNodeUpdated.apply))
    nodeCache.start(StartMode.BUILD_INITIAL_CACHE)

    executorCache = new PathChildrenCache(zk, "/executor", true)
    executorCache.getListenable.addListener(
      new CacheListenerBroadcaster[ExecutorData](FlareExecutorJoined.apply, FlareExecutorExited.apply, FlareExecutorUpdated.apply))
    executorCache.start(StartMode.BUILD_INITIAL_CACHE)
  }

  def start(profile: FlareClusterProfile): Unit = {
    localProfile = Some(profile)
    zk.start()
    startMemberCaches()
    profile.start(this)
    eventBus.start(null)
  }

  def close() = {
    logInfo("Closing flare cluster connection")
    driverCache.close()
    nodeCache.close()
    executorCache.close()
    zk.close()
    eventBus.stop()
  }

  def refreshMembers(): Unit = {
    driverCache.rebuild()
    nodeCache.rebuild()
    executorCache.rebuild()
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
    ser.fromBytes[String](zk.getData.forPath("/app/id"))
  }

  def properties: Map[String, String] = {
    val ser = serializer.newInstance
    ser.fromBytes[Map[String, String]](zk.getData.forPath("/app/properties"))
  }

  def addListener(listener: FlareClusterListener): Unit = {
    eventBus.addListener(listener)
  }

  def counter(name: String): FlareCounter = {
    counterService.create(name)
  }

  private[spark] def register(data: FlareClusterMemberData) = {
    if (localMemberData.isDefined) {
      throw new SparkException("Attempting to register after already being registered")
    }
    localMemberData = Some(data)
    val ser = serializer.newInstance
    zk.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(data.toPath, ser.toBytes(data))
  }

  private[spark] def localData = localMemberData
}

object FlareCluster {
  def apply(conf: FlareClusterConfiguration) = new FlareCluster(conf)
}