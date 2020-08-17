package cn.yintech.online

import java.text.SimpleDateFormat
import java.util.Date

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object KafkaEventLog2Hive {

    def main(args: Array[String]): Unit = {
      // 1.创建SparkConf对象
      val conf: SparkConf = new SparkConf()
        .setAppName("SparkStreamingKafka_Direct")
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
      val ssc: StreamingContext = new StreamingContext(sc, Seconds(60))
      //设置checkpoint目录
      ssc.checkpoint("hdfs:///user/licaishi/KafkaEventLog2Hive_checkpoint")

      // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
      // 4.1.配置kafka相关参数
      val kafkaParams = Map(
        "bootstrap.servers" -> "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092",
        "group.id" -> "KafkaEventLog2Hive",
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
      dstream.foreachRDD(records => {
        if (records.count() > 0) {
          //获取offset
          val offsetRanges = records.asInstanceOf[HasOffsetRanges].offsetRanges
          //写入Hive
          //value为实际操作中的结果集，即是//TODO返回的结果集
          val valueRdd = records.map(record => {

            val nObject: JSONObject = new JSONParser().parse(record.value()).asInstanceOf[JSONObject]
//            var dataMap = jsonParse(record.value())
            val kafka_time = nObject.getOrDefault("time", "0").toString.toLong
            val dt = new SimpleDateFormat("yyyy-MM-dd").format(kafka_time)
            nObject.put("dt" , dt)
            nObject.toJSONString()
//            Row(
//              dataMap.getOrElse("distinct_id", ""),
//              dataMap.getOrElse("tmp_flush_time", ""),
//              dataMap.getOrElse("tmp_track_id", ""),
//              dataMap.getOrElse("event", ""),
//              dataMap.getOrElse("project", ""),
//              dataMap.getOrElse("user_id", ""),
//              dataMap.getOrElse("map_id", ""),
//              dataMap.getOrElse("lib", ""),
//              dataMap.getOrElse("properties", ""),
//              dataMap.getOrElse("project_id", ""),
//              dataMap.getOrElse("ver", ""),
//              dataMap.getOrElse("extractor", ""),
//              dataMap.getOrElse("time", ""),
//              dataMap.getOrElse("type", ""),
//              dataMap.getOrElse("recv_time", ""),
////              kafka_time,
////              system_time,
//              dt
//            )
          })

//         val structType = StructType(Array(
//            StructField("distinct_id", StringType, true),
//            StructField("tmp_flush_time", StringType, true),
//            StructField("tmp_track_id", StringType, true),
//            StructField("event", StringType, true),
//            StructField("project", StringType, true),
//            StructField("user_id", StringType, true),
//            StructField("map_id", StringType, true),
//            StructField("lib", StringType, true),
//            StructField("properties", StringType, true),
//            StructField("project_id", StringType, true),
//            StructField("ver", StringType, true),
//            StructField("extractor", StringType, true),
//            StructField("time", StringType, true),
//            StructField("type", StringType, true),
//            StructField("recv_time", StringType, true),
////            StructField("kafka_time", LongType, true),
////            StructField("system_time", LongType, true),
//            StructField("dt", StringType, true)
//          ))

          //          val subRdd = records.sparkContext.parallelize(value.)
          val sqlContext: SQLContext = new HiveContext(records.sparkContext)
          sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
          sqlContext.setConf("hive.exec.dynamic.partition", "true")
//          val valueDf: DataFrame = sqlContext.createDataFrame(valueRdd, structType)
          val valueDf: DataFrame = sqlContext.read
            .format("json")
            .json(valueRdd)
              .select(
                "distinct_id",
                "_flush_time",
                "_track_id",
                "event",
                "project",
                "user_id",
                "map_id",
                "lib",
                "properties",
                "project_id",
                "ver",
                "extractor",
                "time",
                "type",
                "recv_time",
                "dt"
              )
          valueDf.coalesce(1)
              .write
              .mode(SaveMode.Append)
              .insertInto("dwd.dwd_base_event_streaming_1d")
//          valueDf.write
          //            .mode(SaveMode.Append)
          //            .format("Hive")
          //            .saveAsTable("lcs_test.ods_base_event_log_streaming_sink_hive")
//          valueDf.createOrReplaceGlobalTempView("t")
//          sqlContext.sql("insert into lcs_test.ods_base_event_log_streaming_sink_hive select * from global_temp.t")
          //提交offset
          dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
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

  def jsonParse(value: String): mutable.Map[String, String] = {
    var map = collection.mutable.Map[String, String]()
    val jsonParser = new JSONParser()
    try{
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        val outKey = outIter.next()
        val outValue = if (outJsonObj.get(outKey) != null) outJsonObj.get(outKey).toString else "null"
        map.put (outKey , outValue)
      }
    } catch {
      case ex : Exception => {
        ex.printStackTrace()
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
  def map2Json(map : mutable.Map[String,String]): String = {
    import scala.collection.JavaConversions.mutableMapAsJavaMap
    val jsonString = JSONObject.toJSONString(map)
    jsonString
  }
}
