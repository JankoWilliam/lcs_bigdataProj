package cn.yintech.online

import java.text.SimpleDateFormat

import cn.yintech.redisUtil.RedisClient
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

import scala.collection.mutable
import scala.util.matching.Regex

object NativeVisitListOffline {
  def main(args: Array[String]): Unit = {
    //  获取日期分区参数
    require(!(args == null  || args.length != 2 || args(0) == "" || args(1) == ""), "Required 2 'time' args")
    val pattern = new Regex("^[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d$") // 日期时间格式
    val dateSome1 = pattern findFirstIn args(0)
    require( dateSome1.isDefined , s"Required PARTITION args like 'yyyy-MM-dd HH:mm:ss' but find ${args(0)}")

    val dateSome2 = pattern findFirstIn args(1)
    require(dateSome2.isDefined , s"Required PARTITION args like 'yyyy-MM-dd HH:mm:ss' but find ${args(1)}")

    val startTime =  dateSome1.get
    val endTime =  dateSome2.get

    val sdf1 =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdf2 =  new SimpleDateFormat("yyyy-MM-dd")
    val sdf3 =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val startDt = sdf2.format(sdf1.parse(startTime))
    val endDt = sdf2.format(sdf1.parse(endTime))

    println("update startTime : " + startTime)
    println("update endTime : " + endTime)
    println("update startDt : " + startDt)
    println("update endDt : " + endDt)

    //saprk切入点
    val spark = SparkSession.builder()
      .appName("dwd_base_event_log")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val dstream = spark.sql(s"SELECT event,properties,time FROM dwd.dwd_base_event_1d WHERE dt BETWEEN '$startDt' AND '$endDt' AND time BETWEEN '$startTime' AND '$endTime'")
      .repartition(10)
      .persist(StorageLevel.MEMORY_AND_DISK)

    /**
     ***************************************************************************
     * TD公众号埋点处理流---------------------------------------------------START--
     ***************************************************************************
     */
    val contentsTD= List(
      "金股天下_菜单栏点击",
      "金股天下_推送点击",
      "金股天下_文章点击",
      "金股天下_文章列表访问",
      "金股天下_文章访问"
    )
    val contentsTDBro: Broadcast[List[String]] = spark.sparkContext.broadcast(contentsTD)
    // 5.处理数据：TD公众号埋点
    val valueTD = dstream
      .map(record => {
        (record.getString(0),record.getString(1),record.getString(2))
      })
      .filter(v => v._1 == "TDGZHVisit" || v._1 == "TDGZHClick" || v._1 == "TDVisit" || v._1 == "TDClick")
      .map(v => {
        val properties = jsonParse(v._2)
        (
          properties.getOrElse("wx_uid",""),
          properties.getOrElse("v1_element_content",""),
          properties.getOrElse("v1_message_title",""),
          properties.getOrElse("v1_message_type",""),
          properties.getOrElse("v1_custom_params",""),
          properties.getOrElse("v1_message_id",""),
          v._3
        )
      }).filter(v => contentsTDBro.value.contains(v._2) && v._1 !="" && v._1 != null).persist(StorageLevel.MEMORY_AND_DISK)

    // 文章阅读数统计
    valueTD.filter(v =>  v._2 == "金股天下_文章访问")
      .map(row => (row._6,row._1))
      .groupByKey(r => r._1)
      .count()
      .repartition(10)
      .foreachPartition( rdd  => {
        if (rdd.nonEmpty){
          val jedis = RedisClient.pool.getResource
          rdd.foreach(r => {
            val str = jedis.hget("lcs:td:buried:read:num", r._1)
            val num = if (str == "null" || str == null) 0 else str.toInt
//            println("**********阅读统计：" + r._1 + "原次数:" +num)
//            println("**********阅读统计：" + r._1 + "加次数:" +r._2)
            jedis.hset("lcs:td:buried:read:num",r._1,(r._2 + num).toString)
          })
          jedis.close()
        }
      })
    /**
     ***************************************************************************
     * TD公众号埋点处理流-----------------------------------------------------END--
     ***************************************************************************
     */
    /**
     ***************************************************************************
     * 视频、动态、新闻播放量统计--------------------------------------------Start--
     ***************************************************************************
     */
    // 6.处理数据：视频、动态、新闻播放量记录埋点
    val contentsCount= List(
      "视频_播放页面访问",
      "动态详情页访问"
    )
    val contentsCountBro: Broadcast[List[String]] = spark.sparkContext.broadcast(contentsCount)
    dstream
      .map(record => {
        (record.getString(0),record.getString(1),record.getString(2))
      })
      .filter(v => v._1 == "NativeAppVisit" )
      .map(v => {
        val properties = jsonParse(v._2)
        (
          properties.getOrElse("v1_element_content",""),
          properties.getOrElse("v1_message_title",""),
          properties.getOrElse("v1_message_type",""),
          properties.getOrElse("v1_custom_params",""),
          properties.getOrElse("v1_message_id",""),
          v._3
        )
      }).filter(v => contentsCountBro.value.contains(v._1) && v._5 !="" && v._5 != null)
      .map( v => (
        v._1 match {
          case "视频_播放页面访问" => "vedio"
          case "动态详情页访问" => "dynamic"
        },v._5))
      .groupByKey(v => (v._1,v._2))
      .count()
      .repartition(10)
      .foreachPartition( rdd  => {
        if (rdd.nonEmpty){
          val jedis = RedisClient.pool.getResource
          rdd.foreach(v => {
            val jsonObj = new JSONObject
            jsonObj.put("type",v._1._1)
            jsonObj.put("msg_id",v._1._2)
            jsonObj.put("num",v._2 + "")
//            println("视频、动态、新闻播放量记录埋点:"+jsonObj.toJSONString)
            jedis.rpush("lcs:buried:count:list",jsonObj.toJSONString)
          })
          jedis.close()
        }
      })
    /**
     ***************************************************************************
     * 视频、动态、新闻播放量统计----------------------------------------------End--
     ***************************************************************************
     */

    /**
     ***************************************************************************
     * 观点、动态、视频分享统计  --------------------------------------------Start--
     ***************************************************************************
     */
    // 7.观点、动态、视频分享记录埋点
    val shareContentsCount= List(
      "观点详情页_页中分享按钮",
      "观点详情页_底部栏_分享按钮",
      "动态详情页_底部栏_分享按钮",
      "视频播放详情页_分享按钮"
    )
    val shareContentsCountBro: Broadcast[List[String]] = spark.sparkContext.broadcast(shareContentsCount)
    dstream
      .map(record => {
        (record.getString(0),record.getString(1),record.getString(2))
      })
      .filter(v => v._1 == "NativeAppClick" || v._1 == "H5Click")
      .map(v => {
        val properties = jsonParse(v._2)
        (
          properties.getOrElse("v1_element_content",""),
          properties.getOrElse("v1_message_title",""),
          properties.getOrElse("v1_message_type",""),
          properties.getOrElse("v1_custom_params",""),
          properties.getOrElse("v1_message_id",""),
          properties.getOrElse("v1_lcs_id",""),
          v._3
        )
      }).filter(v => shareContentsCountBro.value.contains(v._1) && v._6 !="" && v._6 != null)
      .map( v => (
        v._1 match {
          case "观点详情页_页中分享按钮" => "view_share_num"
          case "观点详情页_底部栏_分享按钮" => "view_share_num"
          case "动态详情页_底部栏_分享按钮" => "dynamic_share_num"
          case "视频播放详情页_分享按钮" => "video_share_num"
        },v._6,sdf2.format(sdf3.parse(v._7))))
      .groupByKey(v => (v._1,v._2,v._3))
      .count()
      .repartition(10)
      .foreachPartition( rdd  => {
        if (rdd.nonEmpty){
          val jedis = RedisClient.pool.getResource
          rdd.foreach(v => {
            val key = "lcs:planner:stat:" + v._1._3 + ":" + v._1._2
            val num = jedis.hget(key,v._1._1)
            val newNum = (if(null != num) num.toLong else 0L ) + v._2
//            println("观点、动态、视频分享记录埋点"+key,v._1._1,v._2)
            jedis.hset(key,v._1._1,newNum.toString)
          })
          jedis.close()
        }
      })
    /**
     ***************************************************************************
     * 观点、动态、视频分享统计----------------------------------------------End--
     ***************************************************************************
     */


    spark.stop()

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
