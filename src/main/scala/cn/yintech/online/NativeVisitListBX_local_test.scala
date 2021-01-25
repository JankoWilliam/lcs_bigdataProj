package cn.yintech.online

import cn.yintech.redisUtil.RedisClient
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.postfixOps

/**
 * 理财师保险业务埋点日志访问记录、统计引流到线上系统（线上和测试环境）
 *
 */
object NativeVisitListBX_local_test {

    def main(args: Array[String]): Unit = {
      // 1.创建SparkConf对象
      val conf: SparkConf = new SparkConf()
        .setAppName("NativeVisitListBX")
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
//      ssc.checkpoint("hdfs:///user/licaishi/NativeVisitListBX_checkpoint")
      ssc.checkpoint("./mycheck5")

      // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
      // 4.1.配置kafka相关参数
      val kafkaParams = Map(
//        "bootstrap.servers" -> "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092",
        "bootstrap.servers" -> "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092",
        "group.id" -> "NativeVisitListBX_1", // 线上消费者组id：NativeVisitList_1
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      // 4.2.定义topic
      val topics = "sc_md_bx" // 线上数据
      val topics_test = "sc_md_bx_test" // 测试环境

      // kafka流
      val dstream =
        // （线上数据）指定起始消费者偏移消费数据，主方法传参3个分区的三个数字，依次为0,1,2分区
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
          Subscribe[String, String](Set(topics,topics_test), kafkaParams))
      }
      //        .persist(StorageLevel.MEMORY_AND_DISK)


      /**
       ***************************************************************************
       *  保险数据                      ------------------------------------Start--
       ***************************************************************************
       */
      /**
       * const { title, author_name: name, article_id: id, c_time: time } = this.article
       * const { openid, unionid } = this.storeWxInfo
       * this.$sa.visit({
       * v1_element_content: '永盈宝_文章访问',
       * wx_openid: openid || '',
       * wx_uid: unionid || '',
       * v1_message_title: title,
       * v1_message_id: id,
       * v1_lcs_name: name,
       * v1_source: this.from || '',
       * v1_custom_params: time
       * })
       * },
       **/
      // 保险BXVisit和BXClick事件总数据
      val bxDataAll = dstream
        .map(record => {
          val dataMap = jsonParse(record.value())
          (dataMap.getOrElse("event", ""),dataMap.getOrElse("properties", ""),dataMap.getOrElse("time", ""),dataMap.getOrElse("project", ""))
        })
        .filter(v => (v._1 == "BXVisit" || v._1 == "BXClick") && v._4 == "lcs_qijianbx")
        .persist(StorageLevel.MEMORY_AND_DISK)

      // 保险测试环境数据BXVisit和BXClick事件总数据
      val bxDataTestAll = dstream
        .map(record => {
          val dataMap = jsonParse(record.value())
          (dataMap.getOrElse("event", ""),dataMap.getOrElse("properties", ""),dataMap.getOrElse("time", ""),dataMap.getOrElse("project", ""))
        })
        .filter(v => (v._1 == "BXVisit" || v._1 == "BXClick") && v._4 == "lcs_qijianbx_test")
//        .persist(StorageLevel.MEMORY_AND_DISK)



      val bxData = bxDataAll.map(v => {
          val properties = jsonParse(v._2)
          BxData(
            properties.getOrElse("v1_element_content",""),   //1
            properties.getOrElse("v1_message_title",""),     //2
            properties.getOrElse("v1_message_id",""),        //3
            properties.getOrElse("v1_custom_params",""),     //4
            properties.getOrElse("wx_openid",""),            //5
            properties.getOrElse("wx_uid",""),               //6
            properties.getOrElse("v1_lcs_name",""),           //7
            properties.getOrElse("v1_source",""),             //8
            properties.getOrElse("v1_lcs_id",""),             //9
            properties.getOrElse("v1_invest_id",""),          //10
            properties.getOrElse("v1_element_title",""),       //11
            properties.getOrElse("v1_element_type",""),      //12
            properties.getOrElse("v1_is_push",""),           //13
            properties.getOrElse("v1_push_type",""),         //14
            properties.getOrElse("v1_is_live",""),            //15
            v._1,                                             //16 event
            properties.getOrElse("v1_push_title",""),         //17
            properties.getOrElse("v1_page_url",""),            //18
            properties.getOrElse("v1_page_title",""),           //19
            properties.getOrElse("v1_tg_name",""),             //20
            properties.getOrElse("v1_tg_id",""),              //21
            properties.getOrElse("v1_wx_username",""),       //22
            properties.getOrElse("v1_tg_open_id",""),         //23
            properties.getOrElse("sc_comments",""),            //24
            properties.getOrElse("sport_name",""),            //25
            properties.getOrElse("v1_custom_params2",""),    //26
            properties.getOrElse("infor_list",""),            //27
            properties.getOrElse("dt_commit_time",""),            //28
            v._4                                             //29 project
          )
        }).persist(StorageLevel.MEMORY_AND_DISK)


      // 启见保_文章/视频访问等
      // 定义v1_element_content内容
      val contentsBX= List(
        "启见保_文章访问",
        "启见保_视频页面往期点击",
        "启见保_视频页面访问"
      )
      val contentsBXBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsBX)
      bxData.filter(v => contentsBXBro.value.contains(v.v1_element_content) )
        .foreachRDD(lines => {
          //存储到redis
          lines.foreachPartition( rdd  => {
            val list = rdd.toList
            if (list.nonEmpty){
              val jedis = RedisClient.pool.getResource
              list.foreach(v => {
                val jsonObj = new JSONObject
                val `type` = v.v1_element_content match {
                  case "启见保_文章访问" => "view_article"
                  case "启见保_视频页面往期点击" => "video_past_click"
                  case "启见保_视频页面访问" => "video_visit"
                }
                jsonObj.put("type",`type`)
                jsonObj.put("v1_element_content",v.v1_element_content)
                jsonObj.put("v1_message_title",v.v1_message_title)
                jsonObj.put("v1_message_id",v.v1_message_id)
                jsonObj.put("v1_custom_params",v.v1_custom_params)
                jsonObj.put("wx_openid",v.wx_openid)
                jsonObj.put("wx_uid",v.wx_uid)
                jsonObj.put("v1_lcs_name",v.v1_lcs_name)
                jsonObj.put("v1_source",v.v1_source)
                jsonObj.put("v1_lcs_id",v.v1_lcs_id)
                jsonObj.put("v1_invest_id",v.v1_invest_id)
                jsonObj.put("v1_is_live",v.v1_is_live)
                jsonObj.put("v1_page_url",v.v1_page_url)
                jedis.rpush("td:bx:behevior:list",jsonObj.toJSONString)
                // 当测试环境使用
                jedis.rpush("td:bx:behevior:list:test",jsonObj.toJSONString)
              })
              jedis.close()
            }
          })
        })
      // BXVisit v1_is_push: isPush事件
      bxData.filter(v => v.event == "BXVisit" && v.v1_is_push == "1")
        .foreachRDD(lines => {
          //存储到redis
          lines.foreachPartition( rdd  => {
            val list = rdd.toList
            if (list.nonEmpty){
              val jedis = RedisClient.pool.getResource
              list.foreach(v => {
                val jsonObj = new JSONObject
                jsonObj.put("type","isPush")
                jsonObj.put("v1_element_content",v.v1_element_content)
                jsonObj.put("v1_message_title",v.v1_message_title)
                jsonObj.put("v1_message_id",v.v1_message_id)
                jsonObj.put("v1_custom_params",v.v1_custom_params)
                jsonObj.put("wx_openid",v.wx_openid)
                jsonObj.put("wx_uid",v.wx_uid)
                jsonObj.put("v1_lcs_name",v.v1_lcs_name)
                jsonObj.put("v1_source",v.v1_source)
                jsonObj.put("v1_lcs_id",v.v1_lcs_id)
                jsonObj.put("v1_invest_id",v.v1_invest_id)
                jsonObj.put("v1_element_title",v.v1_element_title)
                jsonObj.put("v1_element_type",v.v1_element_type)
                jsonObj.put("v1_is_push",v.v1_is_push)
                jsonObj.put("v1_push_type",v.v1_push_type)
                jsonObj.put("v1_push_title",v.v1_push_title)
                jsonObj.put("v1_page_url",v.v1_page_url)
                jedis.rpush("td:bx:behevior:list",jsonObj.toJSONString)
                // 当测试环境使用
                jedis.rpush("td:bx:behevior:list:test",jsonObj.toJSONString)
              })
              jedis.close()
            }
          })
        })
      // 启见保_营销活动事件
      bxData.filter(v => v.v1_element_type == "启见保_营销活动")
        .foreachRDD(lines => {
          //存储到redis
          lines.foreachPartition( rdd  => {
            val list = rdd.toList
            if (list.nonEmpty){
              val jedis = RedisClient.pool.getResource
              list.foreach(v => {
                val jsonObj = new JSONObject
                val `type` = v.event match {
                  case "BXVisit" => "activity_visit"
                  case "BXClick" => "activity_click"
                }
                jsonObj.put("type",`type`)
                jsonObj.put("v1_element_content",v.v1_element_content)
                jsonObj.put("v1_message_title",v.v1_message_title)
                jsonObj.put("v1_message_id",v.v1_message_id)
                jsonObj.put("v1_custom_params",v.v1_custom_params)
                jsonObj.put("wx_openid",v.wx_openid)
                jsonObj.put("wx_uid",v.wx_uid)
                jsonObj.put("v1_lcs_name",v.v1_lcs_name)
                jsonObj.put("v1_source",v.v1_source)
                jsonObj.put("v1_lcs_id",v.v1_lcs_id)
                jsonObj.put("v1_invest_id",v.v1_invest_id)
                jsonObj.put("v1_element_title",v.v1_element_title)
                jsonObj.put("v1_element_type",v.v1_element_type)
                jsonObj.put("v1_page_url",v.v1_page_url)
                jedis.rpush("td:bx:behevior:list",jsonObj.toJSONString)
                // 当测试环境使用
                jedis.rpush("td:bx:behevior:list:test",jsonObj.toJSONString)
              })
              jedis.close()
            }
          })
        })

      // 展业助手_计划书&视频直播H5访问
      bxData.filter(v => v.v1_element_content == "展业助手_计划书详情页访问"
        || v.v1_element_content == "展业助手_计划书详情页_点击投保"
        || v.v1_element_content == "视频直播H5访问")
        .foreachRDD(lines => {
          //存储到redis
          lines.foreachPartition( rdd  => {
            val list = rdd.toList
            if (list.nonEmpty){
              val jedis = RedisClient.pool.getResource
              list.foreach(v => {
                val jsonObj = new JSONObject
                val `type` = v.v1_element_content match {
                  case "展业助手_计划书详情页访问" => "planbook_visit"
                  case "展业助手_计划书详情页_点击投保" => "planbook_click"
                  case "视频直播H5访问" => "live_h5_visit"
                }
                jsonObj.put("type",`type`)
                jsonObj.put("v1_element_content",v.v1_element_content)
                jsonObj.put("v1_page_title",v.v1_page_title)
                jsonObj.put("v1_tg_name",v.v1_tg_name)
                jsonObj.put("v1_tg_id",v.v1_tg_id)
                jsonObj.put("wx_openid",v.wx_openid)
                jsonObj.put("wx_uid",v.wx_uid)
                jsonObj.put("v1_lcs_name",v.v1_lcs_name)
                jsonObj.put("v1_lcs_id",v.v1_lcs_id)
                jsonObj.put("v1_wx_username",v.v1_wx_username)
                jsonObj.put("v1_page_url",v.v1_page_url)
                jsonObj.put("v1_message_title",v.v1_message_title)
                jsonObj.put("v1_tg_open_id",v.v1_tg_open_id)
                jedis.rpush("td:bx:behevior:list",jsonObj.toJSONString)
                // 当测试环境使用
                jedis.rpush("td:bx:behevior:list:test",jsonObj.toJSONString)
              })
              jedis.close()
            }
          })
        })

      // 启见保_保障测评页面访问
      bxData.filter(v => v.v1_element_content == "启见保_保障测评页面访问")
        .foreachRDD(lines => {
          //存储到redis
          lines.foreachPartition( rdd  => {
            val list = rdd.toList
            if (list.nonEmpty){
              val jedis = RedisClient.pool.getResource
              list.foreach(v => {
                val jsonObj = new JSONObject
                val `type` = v.v1_element_content match {
                  case "启见保_保障测评页面访问" => "se_visit"
                }
                jsonObj.put("type",`type`)
                jsonObj.put("v1_element_content",v.v1_element_content)
                jsonObj.put("v1_page_title",v.v1_page_title)
                jsonObj.put("wx_openid",v.wx_openid)
                jsonObj.put("wx_uid",v.wx_uid)
                jsonObj.put("v1_custom_params",v.v1_custom_params)
                jsonObj.put("v1_page_url",v.v1_page_url)
                jedis.rpush("td:bx:behevior:list",jsonObj.toJSONString)
                // 当测试环境使用
                jedis.rpush("td:bx:behevior:list:test",jsonObj.toJSONString)
              })
              jedis.close()
            }
          })
        })


      // 启见保_推广/营销活动页领取&启见保_推广/营销活动页访问
      bxData.filter(v => v.v1_element_content == "启见保_推广/营销活动页领取"
                || v.v1_element_content == "启见保_推广/营销活动页访问"  )
        .foreachRDD(lines => {
          //存储到redis
          lines.foreachPartition( rdd  => {
            val list = rdd.toList
            if (list.nonEmpty){
              val jedis = RedisClient.pool.getResource
              list.foreach(v => {
                val jsonObj = new JSONObject
                val `type` = v.v1_element_content match {
                  case "启见保_推广/营销活动页领取" => "shanzhen_receive"
                  case "启见保_推广/营销活动页访问" => "shanzhen_visit"
                }
                jsonObj.put("type",`type`)
                jsonObj.put("sc_comments",v.sc_comments)
                jsonObj.put("sport_name",v.sport_name)
                jsonObj.put("infor_list",v.infor_list)
                jsonObj.put("dt_commit_time",v.dt_commit_time)
                jsonObj.put("wx_openid",v.wx_openid)
                jsonObj.put("wx_uid",v.wx_uid)
                jsonObj.put("v1_custom_params",v.v1_custom_params)
                jsonObj.put("v1_custom_params2",v.v1_custom_params2)
                jsonObj.put("v1_page_url",v.v1_page_url)
                jsonObj.put("v1_page_title",v.v1_page_title)
                jsonObj.put("v1_tg_id",v.v1_tg_id)
                jsonObj.put("v1_tg_name",v.v1_page_title)
                jedis.rpush("td:bx:behevior:list",jsonObj.toJSONString)
                // 当测试环境使用
                jedis.rpush("td:bx:behevior:list:test",jsonObj.toJSONString)
              })
              jedis.close()
            }
          })
        })

      /**
       * 保险数据整体引流（properties字段，json串直接打入redis list）
       */
      bxDataAll.foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition( rdd  => {
          val list = rdd.toList
          if (list.nonEmpty){
            val jedis = RedisClient.pool.getResource
            list.foreach(v => {
              val jsonParser = new JSONParser()
              val value = jsonParser.parse(v._2).asInstanceOf[JSONObject]
              value.put("event",v._1)
              jedis.rpush("td:bx:behevior:list:new",value.toJSONString())
              // 当测试环境使用
//              jedis.rpush("td:bx:behevior:list:new:test",v._2)
            })
            jedis.close()
          }
        })
      })
      /**
       * 保险测试环境数据整体引流（properties字段，json串直接打入redis list）
       */
      bxDataTestAll.foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition( rdd  => {
          val list = rdd.toList
          if (list.nonEmpty){
            val jedis = RedisClient.pool.getResource
            list.foreach(v => {
//              jedis.rpush("td:bx:behevior:list:new",v._2)
              val jsonParser = new JSONParser()
              val value = jsonParser.parse(v._2).asInstanceOf[JSONObject]
              value.put("event",v._1)
              val v1_element_content = value.getOrDefault("v1_element_content","")
              if (v1_element_content == "小程序_直播页面访问") {
                println("is_event :" +  v1_element_content)
              }
              else {
                println("not_event :" +  v1_element_content)
              }
              println("all_event :" +  value.toJSONString())
              // 当测试环境使用
              jedis.rpush("td:bx:behevior:list:new:test",value.toJSONString())
            })
            jedis.close()
          }
        })
      })

      /**
       ***************************************************************************
       * 保险数据                         ------------------------------------End--
       ***************************************************************************
       */

      // 9.开启流式计算
      ssc.start()

      // 阻塞一直运行
      ssc.awaitTermination()

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
                     dt_commit_time : String,
                     project : String // kafka数据，神策埋点项目名称
  )

}
