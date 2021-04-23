package cn.yintech.online

import java.sql.{DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat

import cn.yintech.hbase.HbaseUtilsScala
import cn.yintech.hbase.HbaseUtilsScala.scaneByPrefixFilter
import cn.yintech.online.NativeVisitList_bak_20210411_1.jsonParse
import cn.yintech.redisUtil.RedisClient
import cn.yintech.utils.ScalaUtils.getBetweenHalfMinute
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.hbase.TableName
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.language.postfixOps

/**
 * 理财师埋点日志访问记录、统计引流到线上系统
 *      v20200402:理财师_TD公众号&理财师埋点日志访问记录、统计引流到线上系统,任务合并
 */
object NativeVisitList_local_test {

    def main(args: Array[String]): Unit = {
      // 1.创建SparkConf对象
      val conf: SparkConf = new SparkConf()
        .setAppName("NativeVisitList")
        .setMaster("local[*]")
        //单位：毫秒，设置从Kafka拉取数据的超时时间，超时则抛出异常重新启动一个task
        .set("spark.streaming.kafka.consumer.poll.ms", "20000")
        //控制每秒读取Kafka每个Partition最大消息数(2000*3*5=30000)，若Streaming批次为5秒，topic最大分区为3，则每批次最大接收消息数为30000
        .set("spark.streaming.kafka.maxRatePerPartition","4000")
        //开启KryoSerializer序列化
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        //开启反压
        .set("spark.streaming.backpressure.enabled","true")
        //开启推测，防止某节点网络波动或数据倾斜导致处理时间拉长(推测会导致无数据处理的批次，也消耗与上一批次相同的执行时间，但不会超过批次最大时间，可能导致整体处理速度降低)
        .set("spark.speculation","true")


      // 2.创建SparkContext对象
      val sc: SparkContext = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      // 3.创建StreamingContext对象
      val ssc: StreamingContext = new StreamingContext(sc, Seconds(15))
      //设置checkpoint目录
//      ssc.checkpoint("hdfs:///user/licaishi/NativeVisitList_checkpoint")
      ssc.checkpoint("./mycheck6")

      // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
      // 4.1.配置kafka相关参数
      val kafkaParams = Map(
//        "bootstrap.servers" -> "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092",
        "bootstrap.servers" -> "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092",
        "group.id" -> "NativeVisitList_local2", // 线上消费者组id：NativeVisitList_1
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
      )
      // 4.2.定义topic
      val topics = "sc_md" // 线上数据
      val topics_test = "sc_md_lcs_test" // 测试环境

      // kafka流
      val dstream =
        // 指定起始消费者偏移消费数据，主方法传参3个分区的三个数字，依次为0,1,2分区
        if (args != null && args.length == 3 ) {
        val partition0: TopicPartition = new TopicPartition(topics, 0)
        val partition1: TopicPartition = new TopicPartition(topics, 1)
        val partition2: TopicPartition = new TopicPartition(topics, 2)
        var fromOffsets = Map[TopicPartition, Long]()
        fromOffsets += (partition0 -> args(0).toLong)
        fromOffsets += (partition1 -> args(1).toLong)
        fromOffsets += (partition2 -> args(2).toLong)

        KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Set(topics,topics_test), kafkaParams,fromOffsets))

        // 默认消费者偏移消费数据
      } else {

        KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](Set(topics), kafkaParams))
      }
      //        .persist(StorageLevel.MEMORY_AND_DISK)

      /**
       ***************************************************************************
       * 测试数据-----------------------------------------------------------Start--
       ***************************************************************************
       */
      // 5.处理数据:理财师埋点
      // 埋点数据Json解析,包含测试环境和线上环境所有数据，只取event、properties、time、project字段，因多次复用所以持久化
      val lcsEvent = dstream
        .map(record => {
          val dataMap = jsonParse(record.value())
          (dataMap.getOrElse("event", ""), dataMap.getOrElse("properties", ""), dataMap.getOrElse("time", ""), dataMap.getOrElse("project", ""))
        }).persist(StorageLevel.MEMORY_AND_DISK)


      val caishangCourseViewRecord = lcsEvent
        // 线上环境数据
        .filter(v => (v._1 == "CSH5Visit"))
        .map(v => {
          val properties = jsonParse(v._2)
          (
            properties.getOrElse("v1_element_content", ""),
            properties.getOrElse("v1_message_title", ""),
            properties.getOrElse("v1_message_id", ""),
            properties.getOrElse("v1_custom_params", ""),
            properties.getOrElse("v1_custom_params2", ""),
            v._3,
            v._4
          )
        }).filter(v => v._1 == "财商_课程_直播" || v._1 == "财商_课程_回看")
        .persist(StorageLevel.MEMORY_AND_DISK)

      caishangCourseViewRecord.print()




      /**
       ***************************************************************************
       * 测试数据-------------------------------------------------------------End--
       ***************************************************************************
       */

      // 9.开启流式计算
      ssc.start()

      // 阻塞一直运行
      ssc.awaitTermination()

    }

  val updateFunc  = (currValues: Seq[(String,String,String,String,String,String,String,Long,Int)],
                     prevValueState: Option[(String,String,String,String,String,String,String,Long,Int)]) => {
    // 通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    // 已累加的值
    // 返回累加后的结果。是一个Option[T]类型
    val prev = prevValueState.getOrElse(("leave_live","","","","","","",0L,0))
    val enters = currValues.filter(_._1 == "enter_live")
    val ons = currValues.filter(_._1 == "on_video")
    var result = ("","","","","","","",0L,0)

    if (prev._1 == "leave_live" || prev._1 == "") {
      //上次状态为空或"leave_live"，此次状态为进入
      if (enters.nonEmpty) { // 单批次内有多条数据，停留时间初始值取最大最小值之差
        val tempmin = currValues.minBy(_._8)
        val tempMax = currValues.maxBy(_._8)
        result = ("enter_live",tempMax._2,tempMax._3,tempMax._4,tempMax._5,tempMax._6,tempMax._7,tempMax._8,(tempMax._8 - tempmin._8).toInt/1000)
      } else if (ons.nonEmpty){ // 单批次内有多条数据，停留时间初始值取最大最小值之差
        val temp = ons.minBy(_._8)
        val tempMax = ons.maxBy(_._8)
        result = ("enter_live",temp._2,temp._3,temp._4,temp._5,temp._6,temp._7,temp._8,(tempMax._8 - temp._8).toInt/1000)
      }
    } //上次状态为"enter_live"，此次状态更新时间戳
    else if(prev._1 == "enter_live" || prev._1 == "on_live"){
      if (ons.nonEmpty){
        val temp = ons.maxBy(_._8)
        result = ("on_live",temp._2,temp._3,temp._4,temp._5,temp._6,temp._7,temp._8,Math.abs(temp._8-prev._8).toInt/1000+prev._9)
      } else if (enters.isEmpty) {
        result = ("leave_live",prev._2,prev._3,prev._4,prev._5,prev._6,prev._7,prev._8+3000,prev._9+3)
      }
    }
    Some(result)
  }
  val updateFuncTD: (Seq[(String, String)], Option[(mutable.Map[String, String], Long, String)]) => Some[(mutable.Map[String, String], Long, String)] = (currValues: Seq[(String,String)],
                                                                                                                                                       prevValueState: Option[(mutable.Map[String,String],Long,String)]) => {
    // 通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    // 已累加的值
    // 返回累加后的结果。是一个Option[T]类型
    var currMap = currValues.groupBy(_._1).map(v => (v._1,v._2.maxBy(_._2)._2))
    val userMap = prevValueState.getOrElse((mutable.Map[String, String](),0L,"0"))
    var map = userMap._1 // 之前userMap
    var addOne = 0 // 阅读数清零
    var time = userMap._3 // 之前时间戳
    var maxTime = "0" // 该批次的最大时间戳
    if (currMap.nonEmpty) {
      maxTime = currMap.maxBy(_._2)._2 // 该批次的最大时间戳
    }

    currMap.map(v => {

      try {
        if (!map.contains(v._1)) {  // 之前数据无该uid，+1
          addOne += 1
        }  else if ((v._2.toLong - map.getOrElse(v._1,"0").toLong) > 100000) { // 之前阅读数据时间与当前阅读时间之差大于100秒，+1
          addOne += 1
          map += (v._1 -> v._2)
        } else {
          currMap += (v._1 -> map.getOrElse(v._1,"0"))
        }
      } catch {
        case  ex : Exception => {

        }
      }
      time = maxTime
      map = map ++ currMap
      map.filter(v => (maxTime.toLong - v._2.toLong) > 120000)

    })

    Some((map,addOne,time))
  }

//  def getSparkSession(sc: SparkContext): SparkSession = {
//    val sparkSession = SparkSession
//        .builder()
//        .enableHiveSupport()
//        .master("local[*]")
//        .config(sc.getConf)
//        .getOrCreate()
//    sparkSession
//  }

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

  case class BxData(
                     v1_element_content: String,
                     v1_message_title: String,
                     v1_message_id: String,
                     v1_custom_params: String,
                     wx_openid: String,
                     wx_uid: String,
                     v1_lcs_name: String,
                     v1_source: String,
                     v1_lcs_id: String,
                     v1_invest_id: String,
                     v1_element_title: String,
                     v1_element_type: String,
                     v1_is_push: String,
                     v1_push_type: String,
                     v1_is_live: String,
                     event: String,
                     v1_push_title: String,
                     v1_page_url: String,
                     v1_page_title: String,
                     v1_tg_name: String,
                     v1_tg_id: String,
                     v1_wx_username: String,
                     v1_tg_open_id : String,
                     sc_comments : String,
                     sport_name : String,
                     v1_custom_params2 : String,
                     infor_list : String,
                     dt_commit_time : String
  )

}
