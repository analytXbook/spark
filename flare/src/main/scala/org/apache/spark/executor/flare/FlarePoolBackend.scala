package org.apache.spark.executor.flare

import redis.clients.jedis.{Jedis, ZParams}

import scala.collection.JavaConversions._
trait FlarePoolBackend {
  def watch(parentPool: String, pool: String): Unit
  def unwatch(parentPool: String, pool: String): Unit
  def taskStarted(parentPool: String, pool: String): Unit
  def taskEnded(parentPool: String, pool: String): Unit
  def childrenRunningTasks(pool: String): Map[String, Int]
}

case class RedisFlarePoolBackendConfiguration(host: String)

class RedisFlarePoolBackend(conf: RedisFlarePoolBackendConfiguration, executorId: String) extends FlarePoolBackend {
  private val jedis = new Jedis(conf.host)

  private def executorPoolKey(pool: String) = s"flare_pool:executor_$executorId:$pool"
  private def globalPoolKey(pool: String) = s"flare_pool:global:$pool"

  override def watch(parentPool: String, pool: String): Unit = {
    jedis.sadd(executorPoolKey(parentPool), pool)
  }

  override def unwatch(parentPool: String, pool: String): Unit = {
    jedis.srem(executorPoolKey(parentPool), pool)
  }

  override def taskStarted(parentPool: String, pool: String): Unit = {
    jedis.zincrby(globalPoolKey(parentPool), 1, pool)
  }

  override def taskEnded(parentPool: String, pool: String): Unit = {
    jedis.zincrby(globalPoolKey(parentPool), -1, pool)
  }

  override def childrenRunningTasks(pool: String): Map[String, Int] = {
    val transaction = jedis.multi()

    val params = new ZParams
    new ZParams().weightsByDouble(1, 0)

    val outKey = "flare_pool_running_tasks_out"
    transaction.zinterstore(outKey, params, globalPoolKey(pool), executorPoolKey(pool))
    val result = transaction.zrangeWithScores(outKey, 0, -1)

    transaction.exec()

    result.get.foldLeft(Map.empty[String, Int]){
      case (acc, tuple) => acc + (tuple.getElement -> tuple.getScore.toInt)
    }
  }
}

