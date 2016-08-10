package org.apache.spark.flare

import java.util.concurrent.ConcurrentHashMap

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.ZKPaths

import scala.collection.JavaConverters._

class FlareCounterService(zk: CuratorFramework, path: String = "/counter"){
  private val localCounters = new ConcurrentHashMap[String, FlareCounter]().asScala

  def create(name: String): FlareCounter = {
    localCounters.get(name) match {
      case Some(counter) => counter
      case None => {
        val counterPath = ZKPaths.makePath(path, name)
        val atomicLong = new DistributedAtomicLong(
          zk, counterPath, new ExponentialBackoffRetry(1000, 3))

        val counter = new FlareCounter(name, atomicLong)

        localCounters(name) = counter
        counter
      }
    }
  }
}
