package cn.yintech.test

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamDirectKafka {

    def main(args: Array[String]): Unit = {
      // 1.创建SparkConf对象
      val conf: SparkConf = new SparkConf()
        .setAppName("SparkStreamingKafka_Direct")
//        .setMaster("local[*]")
        //单位：毫秒，设置从Kafka拉取数据的超时时间，超时则抛出异常重新启动一个task
        .set("spark.streaming.kafka.consumer.poll.ms", "20000")
        //控制每秒读取Kafka每个Partition最大消息数(500*3*10=15000)，若Streaming批次为10秒，topic最大分区为3，则每批次最大接收消息数为15000
        .set("spark.streaming.kafka.maxRatePerPartition","500")
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
      ssc.checkpoint("hdfs:///user/zhanpeng.fu/mycheck")

      // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
      // 4.1.配置kafka相关参数
      val kafkaParams = Map(
        "bootstrap.servers" -> "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092",
        "group.id" -> "direct_group",
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

      // 5.获取topic中的数据
      val logData = dstream.map(v =>{
        val dataMap = JSON.parseObject(v.value())
        val timestemp = dataMap.get("time").asInstanceOf[Long]
        val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestemp)
        (v.offset(),v.key(),time)
      })
//
//
//      val result :MapWithStateDStream[String, Int, Int, Any] = logData.mapWithState(
//        StateSpec.function((word, one, state) => {
//          val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
//          val output = (word, sum)
//          state.update(sum)
//          output
//        }))

      // 8.通过Output Operations操作打印数据
      logData.print()

      // 9.开启流式计算
      ssc.start()

      // 阻塞一直运行
      ssc.awaitTermination()

    }
}
