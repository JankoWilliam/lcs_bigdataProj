package cn.yintech.online

import java.text.SimpleDateFormat
import java.util.Date

import cn.yintech.redisUtil.RedisClient
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, rdd}

import scala.collection.mutable

/**
 * 理财师_TD公众号埋点日志访问记录、统计引流到线上系统
 *      v20200402:
 */
object NativeVisitListTD {

    def main(args: Array[String]): Unit = {
      // 1.创建SparkConf对象
      val conf: SparkConf = new SparkConf()
        .setAppName("NativeVisitListTD")
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
      val ssc: StreamingContext = new StreamingContext(sc, Seconds(15))
      //设置checkpoint目录
//      ssc.checkpoint("hdfs:///user/licaishi/NativeVisitList_DT_checkpoint")
      ssc.checkpoint("./mycheck6")

      // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
      // 4.1.配置kafka相关参数
      val kafkaParams = Map(
//        "bootstrap.servers" -> "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092",
        "bootstrap.servers" -> "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092",
//        "group.id" -> "NativeVisitList_TD_test02", // 线上消费者组id：NativeVisitListTD_1
        "group.id" -> "NativeVisitListTD_2",
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

      // 定义v1_element_content内容
      val contents01 = List(
        "金股天下_菜单栏点击",
        "金股天下_推送点击",
        "金股天下_文章点击",
        "金股天下_文章列表访问",
        "金股天下_文章访问"
      )
      val contents01Bro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contents01)

      // 5.处理数据
      val value = dstream
          .map(record => {
            val dataMap = jsonParse(record.value())
            (dataMap.getOrElse("event", ""),dataMap.getOrElse("properties", ""),dataMap.getOrElse("time", ""))
          })
          .filter(v => v._1 == "TDGZHVisit" || v._1 == "TDGZHClick" )
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
          }).filter(v => contents01Bro.value.contains(v._2) && v._1 !="" && v._1 != null).cache() // 包含上述几种content的事件

      /**
        TD 项目
        1.用户行为明细redis list , key:   lcs:td:buried:event:list
        {"uid":"11111","type":"","content":"金股天下_菜单栏点击","title":"标题","msg_type":"0","tuisong_param":"全部推送",time":"2020-02-02 11:11:11"}
        type:
           menu:"金股天下_菜单栏点击",
           push:"金股天下_推送点击",
           artiClick:"金股天下_文章点击",
           artiList:"金股天下_文章列表访问"
           artiVisit:"金股天下_文章访问"
        msg_type：消息（0），文本（1），图片（2）
        tuisong_param（限金股天下_推送点击）：全部推送,签到推送

        2.文章阅读数redis hash , key:   lcs:td:buried:read:num
           (key：文章id，value：阅读数)
       *
       */
      // 行为记录数据
      value.map(row => (row._1,row._2,row._3,row._4,row._5,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(row._7.toLong)))
        .foreachRDD(lines => {
          //存储到redis
          lines.foreachPartition( rdd => {
            val jedis = RedisClient.pool.getResource
            rdd.foreach(v =>{
              val jsonObj = new JSONObject
              jsonObj.put("uid",v._1)
              if (v._2 == "金股天下_菜单栏点击")
                jsonObj.put("type","menu")
              else if (v._2 == "金股天下_推送点击")
                jsonObj.put("type","push")
              else if (v._2 == "金股天下_文章点击")
                jsonObj.put("type","artiClick")
              else if (v._2 == "金股天下_文章列表访问")
                jsonObj.put("type","artiList")
              else if (v._2 == "金股天下_文章访问")
                jsonObj.put("type","artiVisit")
              jsonObj.put("content", v._2)
              jsonObj.put("title",v._3)
              jsonObj.put("msg_type", v._4)
              jsonObj.put("tuisong_param",v._5)
              jsonObj.put("time", v._6)
              //      jedis.select(1)
              println("**********行为记录："+jsonObj.toJSONString)
              jedis.rpush("lcs:td:buried:event:list",jsonObj.toJSONString)
            })
            jedis.close()
          })
        })

      // 文章阅读数统计
      import scala.collection.JavaConverters._
      value.filter(v =>  v._2 == "金股天下_文章访问")
        .map(row => (row._6,(row._1,row._7)))
        .updateStateByKey[(mutable.Map[String,String],Long,String)](updateFunc) //类型为key=文章id,value=(Map(用户id,时间),阅读数,最新时间)
        .filter( v => {
            v._2._2 > 0
        }).map(v => (v._1,v._2._2))
          .foreachRDD(lines => {
            //存储到redis
            lines.foreachPartition( rdd  => {
              val list = rdd.toList
              if (list.nonEmpty){
                val jedis = RedisClient.pool.getResource

                list.foreach(r => {
                  val str = jedis.hget("lcs:td:buried:read:num", r._1)
                  val num = if (str == "null" || str == null) 0 else str.toInt
//                  println("**********阅读统计：" + r._1 + "原次数:" +num)
//                  println("**********阅读统计：" + r._1 + "加次数:" +r._2)
                  jedis.hset("lcs:td:buried:read:num",r._1,(r._2 + num).toString)
                })
//                jedis.hset("lcs:td:buried:read:num",rdd.map(v => (v._1,v._2.toString)).toMap.asJava)
                jedis.close()
              }
            })
          })

      // 9.开启流式计算
      ssc.start()

      // 阻塞一直运行
      ssc.awaitTermination()

    }

  val updateFunc: (Seq[(String, String)], Option[(mutable.Map[String, String], Long, String)]) => Some[(mutable.Map[String, String], Long, String)] = (currValues: Seq[(String,String)],
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

}
