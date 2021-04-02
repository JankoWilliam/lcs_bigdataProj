package cn.yintech.test

import java.util.Properties

import cn.yintech.test.FlinkKafkaTest.{MyWindowFunction, TimeStampExtractor, jsonParse}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.hadoop.hive.conf.HiveConf

object FlinkHotItems {

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
//      .allowedLateness(Time.seconds(10))
//      .sideOutputLateData(new OutputTag[((String,String),(String,String))]("late-elements"))
      .apply(new MyWindowFunction())

//    val sink = new BucketingSink[String]("hdfs:///user/zhanpeng.fu/flink_data")
//    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd"))
//    sink.setBatchSize(2 * 1024 )
//    sink.setBatchRolloverInterval(3600000)
//
//    value.map(v => v._1+"|"+v._2+"|"+v._3+"|"+v._4).addSink(sink)

    value.print("data")
//    val lateStream: DataStream[((String,String),(String,String))] = value.getSideOutput(new OutputTag[((String,String),(String,String))]("late-elements"))
//    lateStream.print("late")

    env.execute("Flink Streaming")

  }

}
