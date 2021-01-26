package cn.yintech.test

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import cn.yintech.utils.ScalaUtils._
import cn.yintech.redisUtil.RedisClientNew
import org.apache.spark.sql.SparkSession

object SparkOnRedisToMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkOnRedisToMysql")
      //      .config("spark.default.parallelism","3")
//      .master("local[*]")
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
    // 阿里redis中用户关注TD理财师数量
    val liveData  = spark.sparkContext.fromRedisHash("lcs:live:visit:count")

//    liveData.sortBy(_._1).take(10).foreach(println(_))

    liveData.foreachPartition(rdd => {
        //创建mysql连接
        val connection = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "syl_w", "naAm7kmYgaG7SrkO1mAT")
//        val connection = DriverManager.getConnection("jdbc:mysql://localhost/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root")
        val sql1 =
          """
            |UPDATE lcs_planner_live_info set
            |circle_id = ?,
            |notice_id = ?,
            |p_uid = ?,
            |live_title = ?,
            |view_count = ?,
            |first_view_count = ?,
            |max_living_count = ?,
            |new_follow_count = ?,
            |comment_count = ?,
            |comment_peoples = ?,
            |gift_count = ?,
            |share_count = ?,
            |old_user_staying_average = ?,
            |old_user_staying_max = ?,
            |old_user_staying_min = ?,
            |new_user_staying_average = ?,
            |new_user_staying_max = ?,
            |new_user_staying_min = ?
            |
            | WHERE notice_id = ? ;
            """.stripMargin
         val sql2 = """
            |insert into lcs_planner_live_info (
            |circle_id,
            |notice_id,
            |p_uid,
            |live_title,
            |view_count,
            |first_view_count,
            |max_living_count,
            |new_follow_count,
            |comment_count,
            |comment_peoples,
            |gift_count,
            |share_count,
            |old_user_staying_average,
            |old_user_staying_max,
            |old_user_staying_min,
            |new_user_staying_average,
            |new_user_staying_max,
            |new_user_staying_min,
            |c_time,
            |u_time
            |) SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            |FROM
            |	DUAL
            |WHERE
            |	NOT EXISTS (
            |		SELECT
            |			notice_id
            |		FROM
            |			lcs_planner_live_info
            |		WHERE
            |			notice_id = ?
            |	);
            |""".stripMargin
        rdd.foreach( v => {
          val ps1 = connection.prepareStatement(sql1)
          val ps2 = connection.prepareStatement(sql2)
          val json = jsonParse(v._2)
          val circle_id = json.getOrElse("circle_id","0")
          val notice_id = json.getOrElse("live_id","0")
          val p_uid = json.getOrElse("lcs_id","0")
          val live_title = json.getOrElse("live_title","")
          val view_count = json.getOrElse("view_count","0")
          val first_view_count = json.getOrElse("first_view_count","0")
          val max_living_count = json.getOrElse("max_living_count","0")
          val new_follow_count = json.getOrElse("new_follow_count","0")
          val comment_count = json.getOrElse("comment_count","0")
          val comment_peoples = json.getOrElse("comment_peoples","0")
          val gift_count = json.getOrElse("gift_count","0")
          val share_count = json.getOrElse("share_count","0")
          val old_user_staying_average = json.getOrElse("old_user_staying_average","0")
          val old_user_staying_max = json.getOrElse("old_user_staying_max","0")
          val old_user_staying_min = json.getOrElse("old_user_staying_min","0")
          val new_user_staying_average = json.getOrElse("new_user_staying_average","0")
          val new_user_staying_max = json.getOrElse("new_user_staying_max","0")
          val new_user_staying_min = json.getOrElse("new_user_staying_min","0")

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val nowTime = sdf.format(new Date())

          ps1.setInt(1,circle_id.toInt)
          ps1.setInt(2,notice_id.toInt)
          ps1.setLong(3,p_uid.toLong)
          ps1.setString(4,live_title)
          ps1.setInt(5,view_count.toInt)
          ps1.setInt(6,first_view_count.toInt)
          ps1.setLong(7,max_living_count.toInt)
          ps1.setInt(8,new_follow_count.toInt)
          ps1.setInt(9,comment_count.toInt)
          ps1.setLong(10,comment_peoples.toInt)
          ps1.setInt(11,gift_count.toInt)
          ps1.setInt(12,share_count.toInt)
          ps1.setDouble(13,old_user_staying_average.toDouble)
          ps1.setDouble(14,old_user_staying_max.toDouble)
          ps1.setDouble(15,old_user_staying_min.toDouble)
          ps1.setDouble(16,new_user_staying_average.toDouble)
          ps1.setDouble(17,new_user_staying_max.toDouble)
          ps1.setDouble(18,new_user_staying_min.toDouble)

          ps1.setInt(19,notice_id.toInt)

          ps2.setInt(1,circle_id.toInt)
          ps2.setInt(2,notice_id.toInt)
          ps2.setLong(3,p_uid.toLong)
          ps2.setString(4,live_title)
          ps2.setInt(5,view_count.toInt)
          ps2.setInt(6,first_view_count.toInt)
          ps2.setLong(7,max_living_count.toInt)
          ps2.setInt(8,new_follow_count.toInt)
          ps2.setInt(9,comment_count.toInt)
          ps2.setLong(10,comment_peoples.toInt)
          ps2.setInt(11,gift_count.toInt)
          ps2.setInt(12,share_count.toInt)
          ps2.setDouble(13,old_user_staying_average.toDouble)
          ps2.setDouble(14,old_user_staying_max.toDouble)
          ps2.setDouble(15,old_user_staying_min.toDouble)
          ps2.setDouble(16,new_user_staying_average.toDouble)
          ps2.setDouble(17,new_user_staying_max.toDouble)
          ps2.setDouble(18,new_user_staying_min.toDouble)
          ps2.setString(19,nowTime)
          ps2.setString(20,nowTime)

          ps2.setInt(21,notice_id.toInt)


//          ps1.addBatch()
//          ps2.addBatch()
          ps1.executeUpdate()
          ps1.close()
          ps2.executeUpdate()
          ps2.close()
        })
        connection.close()
      })

//    spark.sparkContext.toRedisHASH()


    spark.stop()

  }
}
