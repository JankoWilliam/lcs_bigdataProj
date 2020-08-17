package cn.yintech.test

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

object KafkaDemoScala {
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties
    //        props.put("bootstrap.servers", "39.107.113.198:9092,59.110.168.230:9092,47.95.237.41:9092");
    props.put("bootstrap.servers", "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092")
    props.put("group.id", "kafka_Direct")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("sc_md"))
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(5000)
      println("count:" + records.count)
      import scala.collection.JavaConversions._
      for (record <- records) {
        println("offset = "+ record.offset +", key = "+ record.key + " time = "+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(record.timestamp()) )
      }
    }
  }
}
