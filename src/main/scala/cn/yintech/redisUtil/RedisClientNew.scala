package cn.yintech.redisUtil


import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClientNew extends Serializable {


  val redisHost: String = "r-2zep1j8uiz10xi8ue7.redis.rds.aliyuncs.com"
  val redisPort: Int = 6379
  val redisTimeout: Int = 30000
  val password: String = null

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