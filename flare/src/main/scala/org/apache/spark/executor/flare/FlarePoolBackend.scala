package org.apache.spark.executor.flare

import org.apache.spark.Logging
import org.apache.spark.scheduler.flare.{FlarePoolDescription, FlareReservationId}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.{Jedis, ZParams}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

trait FlarePoolBackend {
  def initialize(executorId: String): Unit
  def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlarePoolDescription]): Unit
  def removeReservation(reservationId: FlareReservationId): Unit
  def taskLaunched(reservationId: FlareReservationId): Unit
  def taskRejected(reservationId: FlareReservationId): Unit
  def taskFinished(reservationId: FlareReservationId): Unit
  def nextReservation(): Option[FlareReservationId]
  def reset(): Unit
}

case class RedisFlarePoolBackendConfiguration(host: String)

class RedisLuaFlarePoolBackend(conf: RedisFlarePoolBackendConfiguration) extends FlarePoolBackend with Logging {
  private val redis = new Jedis(conf.host)

  private def loadScript(scriptName: String): Unit = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(s"/org/apache/spark/flare/redis/$scriptName.lua"))
    val script = source.mkString
    scriptSHA(scriptName) = redis.scriptLoad(script)
  }

  private val scriptSHA = mutable.Map[String, String]()

  private def evalScript(scriptName: String, args: String*): AnyRef = {
    try {
      redis.evalsha(scriptSHA(scriptName), 0, args: _*)
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"Failed to run script '$scriptName', args = (${args.mkString(" ")})", ex)
    }
  }

  private var executorId: Option[String] = None

  override def initialize(executorId: String): Unit = {
    this.executorId = Some(executorId)

    loadScript("add_reservation")
    loadScript("remove_reservation")
    loadScript("next_reservation")
    loadScript("task_launched")
    loadScript("task_rejected")
    loadScript("task_finished")
  }

  override def reset(): Unit = {
    logInfo("Flushing redis pool backend")
    redis.flushDB()
  }

  implicit def writePoolDescription(pool: FlarePoolDescription): JValue = {
  	("name" -> pool.name) ~
    ("max_share" -> pool.maxShare) ~
    ("min_share" -> pool.minShare) ~
    ("weight" -> pool.weight)
  }

  override def addReservation(reservationId: FlareReservationId, count: Int, groups: Seq[FlarePoolDescription]): Unit = {
    evalScript("add_reservation",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString,
      count.toString,
      compact(render(groups)))
  }

  override def removeReservation(reservationId: FlareReservationId): Unit = {
    evalScript("remove_reservation",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }

  override def nextReservation(): Option[FlareReservationId] = {
    Option(evalScript("next_reservation",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized"))))
      .map(_.asInstanceOf[java.util.List[String]])
      .map(result => FlareReservationId(result(0).toInt, result(1).toInt, result(2).toInt))
  }

  override def taskRejected(reservationId: FlareReservationId): Unit = {
    evalScript("task_rejected",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)

  }

  override def taskLaunched(reservationId: FlareReservationId): Unit = {
    evalScript("task_launched",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)

  }

  override def taskFinished(reservationId: FlareReservationId): Unit = {
    evalScript("task_finished",
      executorId.getOrElse(throw new RuntimeException("Pool backend has not been initialized")),
      reservationId.stageId.toString,
      reservationId.attemptId.toString,
      reservationId.driverId.toString)
  }
}