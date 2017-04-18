package org.apache.spark.flare

import scala.collection.mutable
import scala.io.Source
import _root_.redis.clients.jedis.{Jedis, JedisPool}

case class FlareRedisConfiguration(host: String)

class FlareRedisClient(conf: FlareRedisConfiguration) {
  private val jedisPool = new JedisPool(conf.host)

  private val scriptSHA = mutable.Map[String, String]()

  def loadScript(scriptName: String): Unit = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(s"$scriptName.lua"))
    val script = source.mkString
    scriptSHA(scriptName) = withJedis(_.scriptLoad(script))
  }

  def withJedis[A](f: Jedis => A): A = {
    val jedis = jedisPool.getResource
    try {
      f(jedis)
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
  }

  def flushAll(): Unit = {
    withJedis(_.flushAll())
  }

  def evalScript(scriptName: String, args: String*): AnyRef = {
    try {
      withJedis(_.evalsha(scriptSHA(scriptName), 0, args: _*))
    } catch {
      case ex: Exception =>
        throw new RuntimeException(s"Failed to run script '$scriptName', args = (${args.mkString(" ")})", ex)
    }
  }

  def close(): Unit = {
    jedisPool.destroy()
  }

}
