package org.apache.spark.scheduler.flare

import java.util.concurrent.{ConcurrentHashMap, Executor, Executors}

import org.apache.spark.internal.Logging
import org.apache.spark.util.IdSource

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

private[spark] case class FlareIdRange(start: Long, end: Long) {
  lazy val range = start.until(end)
  lazy val size = range.size
  def iterator = range.iterator
}

private[spark] class FlareIdGroup(idGroup: String, isInt: Boolean, backend: FlareSchedulerBackend) extends Logging {
  implicit private val ec = FlareIdGroup.fetchContext

  private var activeIterator: Iterator[Long] = Iterator.empty

  @volatile private var readyRange: Option[FlareIdRange] = None
  @volatile private var pendingFetch: Option[Promise[FlareIdRange]] = None

  def next(): Long = synchronized {
    if (!activeIterator.hasNext) {
      activeIterator = nextRange().iterator
    }
    activeIterator.next()
  }

  private def fetchNextRange(): Future[FlareIdRange] = {
    val promise = Promise[FlareIdRange]()
    pendingFetch = Some(promise)
    promise.completeWith(backend.allocateIds(idGroup, isInt))
    promise.future.map { range =>
      pendingFetch = None
      range
    }
  }

  private def nextRange(): FlareIdRange = {
    val next = readyRange match {
      case Some(range) => {
        readyRange = None
        range
      }
      case None => {
        pendingFetch match {
          case Some(promise) => {
            val range = Await.result(promise.future, 1 minute)
            readyRange = None
            range
          }
          case None => Await.result(fetchNextRange(), 1 minute)
        }
      }
    }

    if (pendingFetch.isEmpty) {
      fetchNextRange().foreach { range =>
        readyRange = Some(range)
      }
    }

    next
  }
}

private[spark] object FlareIdGroup {
  private val fetchContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
}

private[spark] class FlareIdSource(backend: FlareSchedulerBackend) extends IdSource {
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