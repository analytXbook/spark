package org.apache.spark.flare

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.util.ThreadUtils
import org.jgroups.blocks.atomic.Counter


private[spark] class AsyncCounter(counter: Counter) extends Counter {
  private val localValue = new AtomicLong(counter.get)
  private var lastUpdate = System.currentTimeMillis
  private val TTL = 1000  // 1 second

  private def hasExpired = (System.currentTimeMillis - lastUpdate) > TTL

  private def runUpdate(update: => Long): Unit = {
    AsyncCounter.updateThreadPool.execute(new Runnable {
      def run = {
        localValue.set(update)
        lastUpdate = System.currentTimeMillis
      }
    })
  }

  override def set(value: Long): Unit = {
    runUpdate {
      counter.set(value)
      value
    }
    localValue.set(value)
  }

  override def incrementAndGet(): Long = {
    runUpdate(counter.incrementAndGet)
    localValue.incrementAndGet
  }


  override def get(): Long = {
    if (hasExpired) {
      runUpdate(counter.get)
    }
    localValue.get
  }

  override def compareAndSet(expect: Long, update: Long): Boolean = {
    runUpdate {
      if (counter.compareAndSet(expect, update))
        update
      else
        localValue.get
    }
    localValue.compareAndSet(expect, update)
  }

  override def decrementAndGet(): Long = {
    runUpdate(counter.decrementAndGet)
    localValue.decrementAndGet()
  }

  override def addAndGet(delta: Long): Long = {
    runUpdate(counter.addAndGet(delta))
    localValue.addAndGet(delta)
  }

  override def getName: String = counter.getName
}

object AsyncCounter {
  private val updateThreadPool = ThreadUtils.newDaemonCachedThreadPool("async-counter-update-thread-pool", 4)
}