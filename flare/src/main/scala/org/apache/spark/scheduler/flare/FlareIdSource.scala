package org.apache.spark.scheduler.flare

import java.util.concurrent.{ConcurrentHashMap, Executor, Executors}

import org.apache.spark.util.IdSource

import scala.concurrent.ExecutionContext

case class FlareIdRange(start: Long, end: Long) {
  lazy val range = start.until(end)
  lazy val size = range.size
  def iterator = range.iterator
}

class FlareIdGroup(idGroup: String, isInt: Boolean, backend: FlareSchedulerBackend) {
  var activeIterator: Iterator[Long] = Iterator.empty

  @volatile var nextRange: Option[FlareIdRange] = None
  @volatile var requestPending: Boolean = false

  def next(): Long = synchronized {
    if (!activeIterator.hasNext) {
      fetchNextRange()
      while(nextRange.isEmpty) {
        wait()
      }
      activeIterator = nextRange.get.iterator
      nextRange = None
      fetchNextRange()
    }
    activeIterator.next()
  }

  private def fetchNextRange() = {
    if (nextRange.isEmpty && !requestPending) {
      implicit val ec = FlareIdGroup.fetchContext

      requestPending = true

      backend.allocateIds(idGroup, isInt).map { range =>
        nextRange = Some(range)
        requestPending = false
        notify()
      }
    }
  }
}

object FlareIdGroup {
  val fetchContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
}

class FlareIdSource(backend: FlareSchedulerBackend) extends IdSource {
  val groups = new ConcurrentHashMap[String, FlareIdGroup]

  private def next(idGroup: String, isInt: Boolean): Long = {
    if (!groups.containsKey(idGroup)) {
      groups.putIfAbsent(idGroup, new FlareIdGroup(idGroup, isInt, backend))
    }
    groups.get(idGroup).next()
  }

  override def nextInt(idGroup: String): Int = next(idGroup, true).toInt

  override def nextLong(idGroup: String): Long = next(idGroup, false)
}