package cn.yintech.test

import cn.yintech.redisUtil.RedisClient
import net.minidev.json.JSONObject
import org.apache.spark.sql.SparkSession

object SparkOnRedisTest {

  case class UserBean(name : String , age : Int , Time : Long)

  def main(args: Array[String]): Unit = {
    //saprk切入点
    val spark = SparkSession.builder()
//      .master("local[*]")
      .appName("SparkOnRedisTest")
      .getOrCreate()

    val df = spark.sql("SELECT DISTINCT deviceId as deviceId FROM dwd.distinct_deviceid  where deviceId is not null and deviceId != '' ")
    df.repartition(100).foreachPartition(rdd => {
      val jedis = RedisClient.pool.getResource
      rdd.foreach(v => {
        val deviceId = v.getString(0)
//        jedis.sadd("lcs:ios:device_id:set",deviceId)
        jedis.sadd("lcs:deviceId:set",deviceId)
      })
      jedis.close()
    })
    spark.stop()

  }



}
