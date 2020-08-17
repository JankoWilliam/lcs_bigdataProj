package cn.yintech.online

import java.text.SimpleDateFormat
import java.util.Date

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object OnlinePeopleCountFromKafka {

    def main(args: Array[String]): Unit = {
      // 1.创建SparkConf对象
      val conf: SparkConf = new SparkConf()
        .setAppName("OnlinePeopleCountFromKafka")
//        .setMaster("local[*]")
        //单位：毫秒，设置从Kafka拉取数据的超时时间，超时则抛出异常重新启动一个task
        .set("spark.streaming.kafka.consumer.poll.ms", "20000")
        //控制每秒读取Kafka每个Partition最大消息数(2000*3*5=30000)，若Streaming批次为5秒，topic最大分区为3，则每批次最大接收消息数为30000
        .set("spark.streaming.kafka.maxRatePerPartition","2000")
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
      val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
      //设置checkpoint目录
      ssc.checkpoint("hdfs:///user/zhanpeng.fu/mycheck3")

      // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
      // 4.1.配置kafka相关参数
      val kafkaParams = Map(
        "bootstrap.servers" -> "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092",
        "group.id" -> "direct_group3",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      // 4.2.定义topic
      val topics = Set("sc_md")

      val dstream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))

      // 5.处理数据
      // event数据2hive
      val value = dstream
          .map(record => {
            val dataMap = jsonParse(record.value())
            (dataMap.getOrElse("event", ""),dataMap.getOrElse("properties", ""),dataMap.getOrElse("time", ""))
          })
          .filter(_._1 == "LiveVisit")
          .map(v => {
            val dataMap2 = jsonParse(v._2)
            (dataMap2.getOrElse("v1_lcs_id", ""),dataMap2.getOrElse("v1_element_content",""),dataMap2.getOrElse("v1_message_title",""),dataMap2.getOrElse("deviceId", ""),v._3)
          })
          .filter(v => v._2 == "视频直播播放" && v._1 != "" && v._4 != "")
          .map(v => ((v._1,v._3),(v._4,v._5)))

      val windowValue = value.groupByKeyAndWindow(Seconds(60),Seconds(5))
          .map(v => {
            val deviceAndtime: Iterable[(String, String)] = v._2
            (v._1,deviceAndtime.map(_._1).toSet.size,deviceAndtime.map(_._2).max)
          })

      windowValue.foreachRDD(lines => {
        val result = lines.map(v => Row(v._1._1,v._1._2,v._2,v._3))
        val structType = StructType(Array(
          StructField("v1_lcs_id", StringType, true),
          StructField("v1_message_title", StringType, true),
          StructField("online_counts", IntegerType, true),
          StructField("count_time", StringType, true)
        ))

        val sqlContext: SQLContext = new HiveContext(lines.sparkContext)
//        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        val valueDf: DataFrame = sqlContext.createDataFrame(result, structType)
        valueDf.createOrReplaceGlobalTempView("t")
        sqlContext.sql("insert into lcs_test.dwd_online_count select * from global_temp.t")
      })




      // 8.通过Output Operations操作打印数据
//      logData.print()

      // 9.开启流式计算
      ssc.start()

      // 阻塞一直运行
      ssc.awaitTermination()

    }

  def getSparkSession(sc: SparkContext): SparkSession = {
    val sparkSession = SparkSession
        .builder()
        .enableHiveSupport()
        .master("local[*]")
        .config(sc.getConf)
        .getOrCreate()
    sparkSession
  }

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try{
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        var outKey = outIter.next()
        val outValue = outJsonObj.get(outKey).toString
        if (outKey.startsWith("_")) outKey = "tmp" + outKey
//        outValue = outValue.replace("$", "")
        map += (outKey -> outValue)
      }
    } catch {
      case ex : Exception => {

      }
    }
    map
  }

  case class EventLogBean(
                           distinct_id : String,
                           tmp_flush_time: String,
                           tmp_track_id: String,
                           event: String,
                           project: String,
                           user_id: String,
                           map_id: String,
                           lib: String,
                           properties: String,
                           project_id: String,
                           ver: String,
                           extractor: String,
                           time: String,
                           `type`: String,
                           recv_time: String,
                           kafka_time: Long,
                           system_time: Long,
                           dt : String
                         )
}
