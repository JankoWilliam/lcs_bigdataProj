package cn.yintech.test

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
 
/**
  * 用Flink消费kafka
  */
object ReadingFromKafka {
 
  private val ZOOKEEPER_HOST = "bigdata002.sj.com:2181,bigdata003.sj.com:2181,bigdata004.sj.com:2181"
  private val KAFKA_BROKER = "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092"
  private val TRANSACTION_GROUP = ""



  def main(args : Array[String]){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
 
    // configure Kafka consumer
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "kafka_flink_test06")
    val topic = "sc_md"
 
    //topicd的名字是new，schema默认使用SimpleStringSchema()即可
    val transaction = env
      .addSource(
        new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
      )
 
    transaction.print()
 
    env.execute()
 
  }
 
}
