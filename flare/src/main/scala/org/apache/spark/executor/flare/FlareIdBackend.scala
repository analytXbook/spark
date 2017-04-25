package org.apache.spark.executor.flare

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.flare.FlareRedisClient
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.flare.FlareIdRange

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

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

  private val idCache = TrieMap[(String, Long), Int]()

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
    val driverId = idCache.getOrElseUpdate((idGroup, rangeEnd), {
      logDebug(s"Looking up $idGroup($id) in redis")
      redis.evalScript("lookup_ids", idGroup, rangeEnd.toString).asInstanceOf[Long].toInt
    })

    logDebug(s"lookupDriver: $idGroup($id) => $driverId")

    driverId
  }

  def allocateIds(driverId: Int, idGroup: String, isInt: Boolean): FlareIdRange = {
    val result = redis.evalScript("allocate_ids", driverId.toString, idGroup, isInt.toString, RANGE_SIZE.toString).asInstanceOf[java.util.List[Long]]
    val range = FlareIdRange(result(0), result(1))
    idCache.put((idGroup, range.end), driverId)
    logDebug(s"Driver $driverId allocated $idGroup ids: ${range.start} -> ${range.end}")
    range
  }
}
