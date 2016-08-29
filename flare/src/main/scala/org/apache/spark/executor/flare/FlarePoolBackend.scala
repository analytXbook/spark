package org.apache.spark.executor.flare

import org.apache.spark.Logging
import org.apache.spark.scheduler.flare.{FlarePoolDescription, FlareReservationId}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import redis.clients.jedis.{Jedis, ZParams}

import scala.collection.JavaConversions._
import scala.io.Source

trait FlarePoolBackend {
  def initialize(): Unit
  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlarePoolDescription]): Unit
  def removeReservation(reservationId: FlareReservationId): Unit
  def taskLaunched(reservationId: FlareReservationId): Unit
  def taskRejected(reservationId: FlareReservationId): Unit
  def taskFinished(reservationId: FlareReservationId): Unit
  def nextReservation(): Option[FlareReservationId]
}

case class RedisFlarePoolBackendConfiguration(host: String)

class RedisLuaFlarePoolBackend(conf: RedisFlarePoolBackendConfiguration, executorId: String) extends FlarePoolBackend with Logging {
  private val redis = new Jedis(conf.host)

  private var addReservationSHA, removeReservationSHA, nextReservationSHA, taskLaunchedSHA, taskRejectedSHA, taskFinishedSHA: String = _

  private def loadScript(name: String): String = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(s"/flare/redis/$name.lua"))
    val script = source.mkString
    redis.scriptLoad(script)
  }

  override def initialize(): Unit = {
    addReservationSHA = loadScript("add_reservation")
    removeReservationSHA = loadScript("remove_reservation")
    nextReservationSHA = loadScript("next_reservation")
    taskLaunchedSHA = loadScript("task_launched")
    taskRejectedSHA = loadScript("task_rejected")
    taskFinishedSHA = loadScript("task_finished")
  }

  implicit def writePoolDescription(pool: FlarePoolDescription): JValue = {
  	("name" -> pool.name) ~
    ("max_share" -> pool.maxShare) ~
    ("min_share" -> pool.minShare) ~
    ("weight" -> pool.weight)
  }

  override def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlarePoolDescription]): Unit = {
    redis.evalsha(addReservationSHA, 0,
      executorId,
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString,
      count.toString,
      compact(render(groups)))
  }

  override def nextReservation(): Option[FlareReservationId] = {
    Option(redis.evalsha(nextReservationSHA, 0, executorId))
      .map(_.asInstanceOf[java.util.List[String]].toList)
      .map(result => FlareReservationId(result(0).toInt, result(1).toInt, result(2).toInt))
  }

  override def taskRejected(reservationId: FlareReservationId): Unit = {
    redis.evalsha(taskRejectedSHA, 0,
      executorId,
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }

  override def removeReservation(reservationId: FlareReservationId): Unit = {
    redis.evalsha(removeReservationSHA, 0,
      executorId,
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }

  override def taskLaunched(reservationId: FlareReservationId): Unit = {
    redis.evalsha(taskLaunchedSHA, 0,
      executorId,
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }

  override def taskFinished(reservationId: FlareReservationId): Unit = {
    redis.evalsha(taskFinishedSHA, 0,
      executorId,
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }
}