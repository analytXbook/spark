package org.apache.spark.executor.flare

import org.apache.spark.flare.FlareRedisClient

import scala.language.implicitConversions
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.flare.{FlarePoolDescription, FlareReservationId}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


trait FlarePoolBackend {
  def init(executorId: String): Unit
  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlarePoolDescription]): Unit
  def removeReservation(reservationId: FlareReservationId): Unit
  def taskLaunched(reservationId: FlareReservationId): Unit
  def taskRejected(reservationId: FlareReservationId): Unit
  def taskFinished(reservationId: FlareReservationId): Unit
  def nextReservation(): Option[FlareReservationId]
  def reset(): Unit
}

class RedisFlarePoolBackend(redis: FlareRedisClient) extends FlarePoolBackend with Logging {

  private var executorId: Option[String] = None

  override def init(executorId: String): Unit = {
    this.executorId = Some(executorId)

    redis.loadScript("add_reservation")
    redis.loadScript("remove_reservation")
    redis.loadScript("next_reservation")
    redis.loadScript("task_launched")
    redis.loadScript("task_rejected")
    redis.loadScript("task_finished")
  }

  override def reset(): Unit = {
    logInfo("Flushing redis pool backend")
    redis.withJedis(_.flushDB())
  }

  implicit def writePoolDescription(pool: FlarePoolDescription): JValue = {
  	("name" -> pool.name) ~
    ("max_share" -> pool.maxShare) ~
    ("min_share" -> pool.minShare) ~
    ("weight" -> pool.weight)
  }

  override def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlarePoolDescription]): Unit = {
    redis.evalScript("add_reservation",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString,
      count.toString,
      compact(render(groups)))
  }

  override def removeReservation(reservationId: FlareReservationId): Unit = {
    redis.evalScript("remove_reservation",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }

  override def nextReservation(): Option[FlareReservationId] = {
    Option(redis.evalScript("next_reservation",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized"))))
      .map(_.asInstanceOf[java.util.List[String]])
      .map(result => FlareReservationId(result(0).toInt, result(1).toInt, result(2).toInt))
  }

  override def taskRejected(reservationId: FlareReservationId): Unit = {
    redis.evalScript("task_rejected",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)

  }

  override def taskLaunched(reservationId: FlareReservationId): Unit = {
    redis.evalScript("task_launched",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)

  }

  override def taskFinished(reservationId: FlareReservationId): Unit = {
    redis.evalScript("task_finished",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }
}