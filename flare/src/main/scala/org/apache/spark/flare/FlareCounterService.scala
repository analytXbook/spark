package org.apache.spark.flare

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.ZKPaths

import scala.collection.JavaConverters._

class FlareCounterService(client: CuratorFramework, path: String = "/counters"){
  private val cache = new PathChildrenCache(client, path, true)
  private val localCounters = new ConcurrentHashMap[String, FlareCounter]().asScala

  private val childListener = new PathChildrenCacheListener {
    import PathChildrenCacheEvent.Type._
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case CHILD_UPDATED => {
          val counterName = ZKPaths.getNodeFromPath(event.getData().getPath())
          localCounters.get(counterName).foreach(_.setLocal(bytesToLong(event.getData.getData)))
        }
        case _ => {
          
        }
      }
    }
  }

  def start() = {
    cache.start(true)
    cache.getListenable.addListener(childListener)
  }

  def close() = {
    cache.close()
  }

  def create(name: String, initialValue: Long): FlareCounter = {
    localCounters.get(name) match {
      case Some(counter) => counter
      case None => {
        val counterPath = ZKPaths.makePath(path, name)
        val atomicLong = new DistributedAtomicLong(
          client, counterPath, new ExponentialBackoffRetry(1000, 3))

        val counter = new FlareCounter(name, atomicLong)

        Option(cache.getCurrentData(counterPath)).map(data => bytesToLong(data.getData)) match {
          case Some(existingValue) => counter.setLocal(existingValue)
          case None => counter.initialize(initialValue)
        }

        localCounters(name) = counter
        counter
      }
    }
  }

  private def bytesToLong(data: Array[Byte]): Long = {
    if (data == null || (data.length == 0)) {
      return 0
    }

    val wrapper = ByteBuffer.wrap(data)
    wrapper.getLong
  }
}
