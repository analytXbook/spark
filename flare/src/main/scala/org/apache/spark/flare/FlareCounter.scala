package org.apache.spark.flare

import java.util.concurrent.atomic.AtomicLong

import org.apache.curator.framework.recipes.atomic.{AtomicValue, DistributedAtomicLong}
import org.apache.spark.Logging
import org.apache.spark.util.ThreadUtils

class FlareCounter(name: String, globalValue: DistributedAtomicLong) extends Logging{
  private val localValue = new AtomicLong()

  private def backgroundUpdate(update: DistributedAtomicLong => AtomicValue[java.lang.Long]): Unit = {
    FlareCounter.updateThreadPool.execute(new Runnable {
      def run = {
        try {
          val result = update(globalValue)
          if (result.succeeded()) {
            setLocal(result.postValue())
          } else {
            logError(s"Failed to run counter update on $name")
          }
        } catch {
          case e: Exception => logError("Failed to run counter update", e)
        }
      }
    })
  }

  private[flare] def setLocal(value: Long) = {
    localValue.set(value)
  }

  private[flare] def initialize(initialValue: Long) = {
    setLocal(initialValue)
  }

  def get(): Long = {
    localValue.get()
  }

  def getAtomic(): Long = {
    val value = globalValue.get().postValue()
    setLocal(value)
    value
  }

  def set(value: Long): Unit = {
    backgroundUpdate(_.trySet(value))
    setLocal(value)
  }

  def setAtomic(value: Long): Unit = {
    setLocal(globalValue.trySet(value).postValue())
  }

  def increment(): Long = {
    backgroundUpdate(_.increment())
    localValue.incrementAndGet()
  }

  def incrementAtomic(): Long = {
    val result = globalValue.increment()
    if (!result.succeeded()) {
      logWarning("Atomic increment failed")
    }

    val value = result.postValue()
    setLocal(value)
    value
  }

  def decrement(): Long = {
    backgroundUpdate(_.decrement())
    localValue.decrementAndGet()
  }

  def decrementAtomic(): Long = {
    val value = globalValue.decrement().postValue()
    setLocal(value)
    value
  }

  def add(delta: Long): Long = {
    backgroundUpdate(_.add(delta))
    localValue.addAndGet(delta)
  }

  def addAtomic(delta: Long): Long = {
    val value = globalValue.add(delta).postValue()
    setLocal(value)
    value
  }

  def subtract(delta: Long): Long = {
    backgroundUpdate(_.subtract(delta))
    localValue.addAndGet(delta * -1)
  }

  def subtractAtomic(delta: Long): Long = {
    val value = globalValue.subtract(delta).postValue()
    setLocal(value)
    value
  }
}

object FlareCounter {
  private val updateThreadPool = ThreadUtils.newDaemonCachedThreadPool("async-counter-update-thread-pool", 1)
}
