package cn.yintech.test

import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import breeze.linalg.max
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector


object FlinkKafkaTest {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getConfig.setAutoWatermarkInterval(5000)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "kafka_flink_test06")
    val topic = "sc_md"

    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    //指定偏移量
    //    myConsumer.setStartFromEarliest() //从最早的数据开始
    myConsumer.setStartFromLatest() //从最新的数据开始
    val stream = env.addSource(myConsumer).assignTimestampsAndWatermarks(new TimeStampExtractor)
//      .assignAscendingTimestamps(jsonParse(_).getOrElse("time", "0").toLong)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
//        override def extractTimestamp(t: String): Long = {
//          //返回指定的字段作为水印字段，这里设置为10秒延迟
//          jsonParse(t).getOrElse("time", "0").toLong
//        }
//      })

    val value = stream.map(record => {
      val dataMap = jsonParse(record)
      (dataMap.getOrElse("event", ""), dataMap.getOrElse("properties", ""), dataMap.getOrElse("time", ""), dataMap.getOrElse("distinct_id", ""))
    })
      .filter(_._1 == "LiveVisit")
      .map(v => {
        val dataMap2 = jsonParse(v._2)
        (dataMap2.getOrElse("v1_lcs_id", ""),
          dataMap2.getOrElse("v1_element_content", ""),
          dataMap2.getOrElse("v1_message_title", ""),
          v._4,
          v._3)
      })
      .filter(v => v._2 == "视频直播播放" && v._1 != "" && v._4 != "")
      .map(v => ((v._1, v._3), (v._4, v._5))) //((lcs_id,标题),(设备id,时间))
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.seconds(10))
      .sideOutputLateData(new OutputTag[((String,String),(String,String))]("late-elements"))
      .apply(new MyWindowFunction())

    val sink = new BucketingSink[String]("hdfs:///user/zhanpeng.fu/flink_data")
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd"))
    sink.setBatchSize(2 * 1024 )
    sink.setBatchRolloverInterval(3600000)

    value.map(v => v._1+"|"+v._2+"|"+v._3+"|"+v._4).addSink(sink)

//    val lateStream: DataStream[((String,String),(String,String))] = value.getSideOutput(new OutputTag[((String,String),(String,String))]("late-elements"))
//    lateStream.print()

    env.execute("Flink Streaming")

  }

  class MyWindowFunction extends WindowFunction[((String, String), (String, String)), (String, String, String, Int), Tuple, TimeWindow] {
    def apply(key: Tuple, window: TimeWindow, input: Iterable[((String, String), (String, String))], out: Collector[(String, String, String, Int)]) {

      val count = input.map(_._2._1).toSet.size
      val k = input.maxBy(_._2._2)
      //      val k = input.iterator().next()._1
      //      for (in <- input.asScala) {
      //        count = count + 1
      //      }

      out.collect((k._1._1, k._1._2, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(k._2._2.toLong), count))
    }

  }

  class TimeStampExtractor extends AssignerWithPeriodicWatermarks[String] with Serializable {

    val maxOutOfOrderness = 30000L // 3.5 seconds
    var currentMaxTimestamp: Long = _
    var a: Watermark = null

    override def getCurrentWatermark: Watermark = {
      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      a
    }

    override def extractTimestamp(t: String, l: Long): Long = {

      val timestamp = jsonParse(t).getOrElse("time", null).toLong
//      val timestamp = new Date().getTime
      currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
//      println(t)
//      println("timestamp:" + sdf.format(timestamp) + "|currentMaxTimestamp:" + sdf.format(currentMaxTimestamp) + "|Watermark:" + a.toString+"--"+sdf.format(a.getTimestamp) + "|systime:" + sdf.format(new Date) )
//      println("timestamp:" + sdf.format(timestamp) + "|currentMaxTimestamp:" + sdf.format(currentMaxTimestamp) + "|Watermark:" + a.toString+"--"+sdf.format(a.getTimestamp) )
      timestamp
    }

  }


//class TimeStampExtractor extends AscendingTimestampExtractor[String] with Serializable {
//  override def extractAscendingTimestamp(t: String): Long = jsonParse(t).getOrElse("time", "0").toLong
//}

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try {
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        val outKey = outIter.next()
        val outValue = if (outJsonObj.get(outKey) != null) outJsonObj.get(outKey).toString else "null"
        map += (outKey -> outValue)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    map
  }
}
