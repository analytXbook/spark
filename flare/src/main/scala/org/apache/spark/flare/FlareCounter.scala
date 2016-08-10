package org.apache.spark.flare

import java.util.concurrent.atomic.AtomicLong

import org.apache.curator.framework.recipes.atomic.{AtomicValue, DistributedAtomicLong}
import org.apache.spark.Logging
import org.apache.spark.util.ThreadUtils

class FlareCounter(name: String, atomicValue: DistributedAtomicLong) extends Logging{
  def get(): Long = {
    logDebug(s"$name get")
    atomicValue.get().postValue()
  }

  def set(value: Long): Unit = {
    var result = atomicValue.trySet(value)
    while(!result.succeeded()) {
      result = atomicValue.trySet(value)
    }
    logDebug(s"$name set took ${result.getStats.getOptimisticTries} attempts (${result.getStats.getOptimisticTimeMs} ms)")
  }

  def increment(): Long = {
    var result = atomicValue.increment()
    while (!result.succeeded()) {
      result = atomicValue.increment()
    }
    logDebug(s"$name increment took ${result.getStats.getOptimisticTries} attempts (${result.getStats.getOptimisticTimeMs} ms)")

    result.postValue()
  }

  def decrement(): Long = {
    var result = atomicValue.decrement()
    while (!result.succeeded()) {
      result = atomicValue.decrement()
    }
    logDebug(s"$name decrement took ${result.getStats.getOptimisticTries} attempts (${result.getStats.getOptimisticTimeMs} ms)")

    result.postValue()
  }

  def add(delta: Long): Long = {
    var result = atomicValue.add(delta)
    while (!result.succeeded()) {
      result = atomicValue.add(delta)
    }
    logDebug(s"$name add took ${result.getStats.getOptimisticTries} attempts (${result.getStats.getOptimisticTimeMs} ms)")
    result.postValue()
  }
}