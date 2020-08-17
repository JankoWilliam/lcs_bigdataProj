package cn.yintech.test

import cn.yintech.redisUtil.RedisClientNew
import org.apache.spark.sql.SparkSession

object SparkOnRedis2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UserJudgeScoring")
      //      .config("spark.default.parallelism","3")
      .master("local[*]")
      .config("spark.redis.host","47.104.254.17")
      .config("spark.redis.port", "9701")
      .config("spark.redis.auth","fG2@bE1^hE4[") //指定redis密码
      //          .config("spark.redis.host","192.168.19.123")
      //          .config("spark.redis.port", "9700")
      //          .config("spark.redis.auth","cW%Kiiuz3Q2aLylk7y%M") //指定redis密码
      //          .config("spark.redis.db","0") //指定redis库
      .enableHiveSupport()
      .getOrCreate()


    import com.redislabs.provider.redis._
    import spark.implicits._
    // 阿里redis中用户关注TD理财师数量
    val liveData  = spark.sparkContext.fromRedisHash("lcs:live:visit:count:circle")
      .foreachPartition(rdd => {
        rdd.foreach( v => {
          val jedis = RedisClientNew.pool.getResource
          jedis.hset("lcs:live:visit:count:circle", v._1 , v._2)
          jedis.close()
        })
      })

//    spark.sparkContext.toRedisHASH()


    spark.stop()

  }
}
