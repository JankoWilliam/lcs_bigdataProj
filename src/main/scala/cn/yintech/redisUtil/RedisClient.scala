package cn.yintech.redisUtil

import java.io.FileInputStream
import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable {

  val propertiesFile = new Properties()
  val path: String = Thread.currentThread().getContextClassLoader.getResource("application.properties").getPath
  propertiesFile.load(new FileInputStream(path))

  val redisHost: String = propertiesFile.getProperty("redis.redisHost")
  val redisPort: Int = propertiesFile.getProperty("redis.redisPort").toInt
  val redisTimeout: Int = propertiesFile.getProperty("redis.redisTimeout").toInt
  val password: String = propertiesFile.getProperty("redis.password")

//  val redisHost = ""
//    val redisPort = 0
//    val redisTimeout = 0
//    val password = ""

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout ,password)
 
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}