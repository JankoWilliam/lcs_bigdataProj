package cn.yintech.eventLog

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.yintech.redisUtil.RedisClient
import net.minidev.json.{JSONArray, JSONObject}
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object UserJudgeScoring {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UserJudgeScoring")
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
    // 取昨日的日期
    val  sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val dt = sdf.format(cal.getTime)

    import com.redislabs.provider.redis._
    import spark.implicits._
    // 阿里redis中用户关注TD理财师数量
    val userFollows  = spark.sparkContext.fromRedisHash("lcs:td:attention:count")
      .toDF("userId","follows").cache()
    // 阿里redis中原有的用户评分数据
    val userScoreBefore  = spark.sparkContext.fromRedisHash("lcs:td:user:judge:score")
      .toDF("id","scoreBefore")

    // 用户关注TD理财师数量入库
    userFollows.createOrReplaceGlobalTempView("userFollows")
    userFollows
      .write
      .mode(SaveMode.Overwrite)
      .format("Hive")
      .saveAsTable("dwd.dwd_user_follows_1d")
//    // 用户评分标签明细表
    val userJudgeScoringDF = spark.sql(
      """
        |SELECT t.id,
        |       CASE
        |           WHEN a.is_visit_contractvarieties = 1 THEN 10
        |           WHEN a.is_visit_contractvarieties is NULL AND b.is_visit_contractvarieties = 1 THEN 9
        |           WHEN a.is_visit_contractvarieties = 0 AND b.is_visit_contractvarieties = 1 THEN 8
        |           WHEN a.is_visit_contractvarieties is NULL AND c.is_visit_contractvarieties = 1 THEN 7
        |           WHEN a.is_visit_contractvarieties = 0 AND c.is_visit_contractvarieties = 1 THEN 6
        |           WHEN a.is_visit_contractvarieties is NULL AND d.is_visit_contractvarieties = 1 THEN 5
        |           WHEN a.is_visit_contractvarieties is NULL AND e.is_visit_contractvarieties = 1 THEN 3
        |           ELSE 0
        |       END AS is_visit_contractvarieties_score,
        |       CASE
        |           WHEN a.is_visit_marketprice = 1 THEN 10
        |           WHEN a.is_visit_marketprice is NULL AND b.is_visit_marketprice = 1 THEN 9
        |           WHEN a.is_visit_marketprice = 0 AND b.is_visit_marketprice = 1 THEN 8
        |           WHEN a.is_visit_marketprice is NULL AND c.is_visit_marketprice = 1 THEN 7
        |           WHEN a.is_visit_marketprice = 0 AND c.is_visit_marketprice = 1 THEN 6
        |           WHEN a.is_visit_marketprice is NULL AND d.is_visit_marketprice = 1 THEN 5
        |           WHEN a.is_visit_marketprice is NULL AND e.is_visit_marketprice = 1 THEN 3
        |           ELSE 0
        |       END AS is_visit_marketprice_score,
        |              CASE
        |           WHEN a.is_visit_account = 1 THEN 10
        |           WHEN a.is_visit_account is NULL AND b.is_visit_account = 1 THEN 9
        |           WHEN a.is_visit_account = 0 AND b.is_visit_account = 1 THEN 8
        |           WHEN a.is_visit_account is NULL AND c.is_visit_account = 1 THEN 7
        |           WHEN a.is_visit_account = 0 AND c.is_visit_account = 1 THEN 6
        |           WHEN a.is_visit_account is NULL AND d.is_visit_account = 1 THEN 5
        |           WHEN a.is_visit_account is NULL AND e.is_visit_account = 1 THEN 3
        |           ELSE 0
        |       END AS is_visit_account_score,
        |       CASE
        |           WHEN a.is_visit_lcspage = 1 THEN 10
        |           WHEN a.is_visit_lcspage is NULL AND b.is_visit_lcspage = 1 THEN 9
        |           WHEN a.is_visit_lcspage = 0 AND b.is_visit_lcspage = 1 THEN 8
        |           WHEN a.is_visit_lcspage is NULL AND c.is_visit_lcspage = 1 THEN 7
        |           WHEN a.is_visit_lcspage = 0 AND c.is_visit_lcspage = 1 THEN 6
        |           WHEN a.is_visit_lcspage is NULL AND d.is_visit_lcspage = 1 THEN 5
        |           WHEN a.is_visit_lcspage is NULL AND e.is_visit_lcspage = 1 THEN 3
        |           ELSE 0
        |       END AS is_visit_lcspage_score,
        |       CASE
        |           WHEN a.is_visit_tdlive = 1 THEN 10
        |           WHEN a.is_visit_tdlive is NULL AND b.is_visit_tdlive = 1 THEN 9
        |           WHEN a.is_visit_tdlive = 0 AND b.is_visit_tdlive = 1 THEN 8
        |           WHEN a.is_visit_tdlive is NULL AND c.is_visit_tdlive = 1 THEN 7
        |           WHEN a.is_visit_tdlive = 0 AND c.is_visit_tdlive = 1 THEN 6
        |           WHEN a.is_visit_tdlive is NULL AND d.is_visit_tdlive = 1 THEN 5
        |           WHEN a.is_visit_tdlive is NULL AND e.is_visit_tdlive = 1 THEN 3
        |           ELSE 0
        |       END AS is_visit_tdlive_score,
        |       CASE
        |           WHEN a.is_visit_tdinformation = 1 THEN 10
        |           WHEN a.is_visit_tdinformation is NULL AND b.is_visit_tdinformation = 1 THEN 9
        |           WHEN a.is_visit_tdinformation = 0 AND b.is_visit_tdinformation = 1 THEN 8
        |           WHEN a.is_visit_tdinformation is NULL AND c.is_visit_tdinformation = 1 THEN 7
        |           WHEN a.is_visit_tdinformation = 0 AND c.is_visit_tdinformation = 1 THEN 6
        |           WHEN a.is_visit_tdinformation is NULL AND d.is_visit_tdinformation = 1 THEN 5
        |           WHEN a.is_visit_tdinformation is NULL AND e.is_visit_tdinformation = 1 THEN 3
        |           ELSE 0
        |       END AS is_visit_tdinformation_score,
        |	   CASE
        |           WHEN f.follows > 5 THEN 50
        |           WHEN f.follows BETWEEN 3 AND 5 THEN 40
        |           WHEN f.follows BETWEEN 1 AND 2 THEN 20
        |		   ELSE 0
        |	   END AS user_follows_score
        |FROM
        |  (SELECT DISTINCT id
        |   FROM dws. dws_td_user_lyering_label_1d
        |   WHERE dt <= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),30) )t
        |LEFT JOIN
        |  (SELECT id,
        |          max(is_visit_contractvarieties) is_visit_contractvarieties,
        |          max(is_visit_marketprice) is_visit_marketprice,
        |          max(is_visit_account) is_visit_account,
        |          max(is_visit_lcspage) is_visit_lcspage,
        |          max(is_visit_tdlive) is_visit_tdlive,
        |          max(is_visit_tdinformation) is_visit_tdinformation
        |   FROM dws. dws_td_user_lyering_label_1d
        |   WHERE dt = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)
        |   GROUP BY id) a ON t.id = a.id
        |LEFT JOIN
        |  (SELECT id,
        |          max(is_visit_contractvarieties) is_visit_contractvarieties,
        |          max(is_visit_marketprice) is_visit_marketprice,
        |          max(is_visit_account) is_visit_account,
        |          max(is_visit_lcspage) is_visit_lcspage,
        |          max(is_visit_tdlive) is_visit_tdlive,
        |          max(is_visit_tdinformation) is_visit_tdinformation
        |   FROM dws. dws_td_user_lyering_label_1d
        |   WHERE dt BETWEEN date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),3) AND date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),2)
        |   GROUP BY id) b ON t.id = b.id
        |LEFT JOIN
        |  (SELECT id,
        |          max(is_visit_contractvarieties) is_visit_contractvarieties,
        |          max(is_visit_marketprice) is_visit_marketprice,
        |          max(is_visit_account) is_visit_account,
        |          max(is_visit_lcspage) is_visit_lcspage,
        |          max(is_visit_tdlive) is_visit_tdlive,
        |          max(is_visit_tdinformation) is_visit_tdinformation
        |   FROM dws. dws_td_user_lyering_label_1d
        |   WHERE dt BETWEEN date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),7) AND date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),4)
        |   GROUP BY id) c ON t.id = c.id
        |LEFT JOIN
        |  (SELECT id,
        |          max(is_visit_contractvarieties) is_visit_contractvarieties,
        |          max(is_visit_marketprice) is_visit_marketprice,
        |          max(is_visit_account) is_visit_account,
        |          max(is_visit_lcspage) is_visit_lcspage,
        |          max(is_visit_tdlive) is_visit_tdlive,
        |          max(is_visit_tdinformation) is_visit_tdinformation
        |   FROM dws. dws_td_user_lyering_label_1d
        |   WHERE dt BETWEEN date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),15) AND date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),8)
        |   GROUP BY id) d ON t.id = d.id
        |LEFT JOIN
        |  (SELECT id,
        |          max(is_visit_contractvarieties) is_visit_contractvarieties,
        |          max(is_visit_marketprice) is_visit_marketprice,
        |          max(is_visit_account) is_visit_account,
        |          max(is_visit_lcspage) is_visit_lcspage,
        |          max(is_visit_tdlive) is_visit_tdlive,
        |          max(is_visit_tdinformation) is_visit_tdinformation
        |   FROM dws. dws_td_user_lyering_label_1d
        |   WHERE dt BETWEEN date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),30) AND date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),16)
        |   GROUP BY id) e ON t.id = e.id
        |LEFT JOIN
        |	global_temp.userFollows f
        |	ON t.id = f.userId
        |""".stripMargin)
    userJudgeScoringDF.createOrReplaceGlobalTempView("userJudgeScoring")
    // 用户评分标签明细表入库
    spark.sql(s"insert OVERWRITE table ads.ads_td_user_judge_scoring_1d partition(dt = '$dt') select * from global_temp.userJudgeScoring")

    // 前一日数据跟今日数据全连接，今日为评分的id赋值0分
    val userJudgeScoringTotalDF =  userJudgeScoringDF.map(row => (row.getString(0),row.getInt(1)+row.getInt(2)+row.getInt(3)+row.getInt(4)+row.getInt(5)+row.getInt(6)+row.getInt(7)))
      .toDF("id","score")
    val result = userScoreBefore.join(userJudgeScoringTotalDF,Seq("id","id"),"full")
      .map(row => {
          (row.getString(0),(if (row(3) == null) 0 else row.getInt(3)).toString)
        })

    // 用户评分标签明细表写入阿里redis
    result.filter(row => row._1 != null && row._1 != "").repartition(50)
      .foreachPartition( rdd  => {
        val jedis = RedisClient.pool.getResource
        rdd.foreach(r => {
          jedis.hset("lcs:td:user:judge:score", r._1, r._2)
        })
        jedis.close()
      })

    spark.stop()

    val jedis2 = RedisClient.pool.getResource
    jedis2.hset("lcs:td:user:judge:score", "update:time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime))
    jedis2.close()
  }

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try{
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        val outKey = outIter.next()
        val outValue = if (outJsonObj.get(outKey) != null) outJsonObj.get(outKey).toString else "null"
        map += (outKey -> outValue)
      }
    } catch {
      case ex : Exception => {
        ex.printStackTrace()
      }
    }
    map
  }
}
