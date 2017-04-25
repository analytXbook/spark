package org.apache.spark.executor.flare

import org.apache.spark.flare.FlareRedisClient
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.flare.FlareIdRange

import scala.collection.JavaConversions._
import scala.collection.mutable

trait FlareIdBackend{
  def init(): Unit
  def lookupDriver(id: Long, idGroup: String, isInt: Boolean): Int
  def allocateIds(driverId: Int, idGroup: String, isInt: Boolean): FlareIdRange
}

class RedisFlareIdBackend(redis: FlareRedisClient) extends FlareIdBackend with Logging {
  val RANGE_SIZE: Long = 1000000l

  override def init(): Unit = {
    redis.loadScript("allocate_ids")
    redis.loadScript("lookup_ids")
  }

  private val idCache = mutable.Map[String, mutable.Map[Long, Int]]().withDefaultValue(mutable.Map[Long, Int]())

  private def rangeBounds(id: Long, isInt: Boolean): (Long, Long) = {
    val factor = (id / RANGE_SIZE)
    val start = factor * RANGE_SIZE
    val end = {
      var value = ((factor + 1) * RANGE_SIZE) - 1
      if (isInt && value > Int.MaxValue) {
        value = Int.MaxValue.toLong
      }
      value
    }
    (start, end)
  }

  def lookupDriver(id: Long, idGroup: String, isInt: Boolean): Int = {
    val rangeEnd = rangeBounds(id, isInt)._2
    val driverId = idCache(idGroup).get(rangeEnd) match {
      case Some(driverId) => driverId
      case None => {
        logDebug(s"Looking up driver for $idGroup $id")
        val driverId = redis.evalScript("lookup_ids", idGroup, rangeEnd.toString).asInstanceOf[Long].toInt
        idCache(idGroup).putIfAbsent(rangeEnd, driverId)
        driverId
      }
    }

    logDebug(s"lookupDriver: $idGroup($id} => $driverId")

    driverId
  }

  def allocateIds(driverId: Int, idGroup: String, isInt: Boolean): FlareIdRange = {
    val result = redis.evalScript("allocate_ids", driverId.toString, idGroup, isInt.toString, RANGE_SIZE.toString).asInstanceOf[java.util.List[Long]]
    val range = FlareIdRange(result(0), result(1))
    idCache(idGroup).putIfAbsent(range.end, driverId)
    logDebug(s"Allocated ids for driver $driverId, group '$idGroup': ${range.start} -> ${range.end}")
    range
  }
}
