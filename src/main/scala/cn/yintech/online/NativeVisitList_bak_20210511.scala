package cn.yintech.online

import java.sql.{DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat

import cn.yintech.hbase.HbaseUtilsScala
import cn.yintech.hbase.HbaseUtilsScala.scaneByPrefixFilter
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
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.language.postfixOps

/**
 * 理财师埋点日志访问记录、统计引流到线上系统
 * v20200402:理财师_TD公众号&理财师埋点日志访问记录、统计引流到线上系统,任务合并
 */
object NativeVisitList_bak_20210511 {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象
    val conf: SparkConf = new SparkConf()
      .setAppName("NativeVisitList")
      //        .setMaster("local[*]")
      //单位：毫秒，设置从Kafka拉取数据的超时时间，超时则抛出异常重新启动一个task
      .set("spark.streaming.kafka.consumer.poll.ms", "20000")
      //控制每秒读取Kafka每个Partition最大消息数(2000*3*5=30000)，若Streaming批次为5秒，topic最大分区为3，则每批次最大接收消息数为30000
      .set("spark.streaming.kafka.maxRatePerPartition", "4000")
      //开启KryoSerializer序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //开启反压
      .set("spark.streaming.backpressure.enabled", "true")
      //开启推测，防止某节点网络波动或数据倾斜导致处理时间拉长(推测会导致无数据处理的批次，也消耗与上一批次相同的执行时间，但不会超过批次最大时间，可能导致整体处理速度降低)
      .set("spark.speculation", "true")


    // 2.创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // 3.创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(15))
    //设置checkpoint目录
    ssc.checkpoint("hdfs:///user/licaishi/NativeVisitList_checkpoint")
    //      ssc.checkpoint("./mycheck5")

    // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
    // 4.1.配置kafka相关参数
    val kafkaParams = Map(
      "bootstrap.servers" -> "192.168.195.211:9092,192.168.195.213:9092,192.168.195.214:9092",
      //        "bootstrap.servers" -> "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092",
      "group.id" -> "NativeVisitList_1", // 线上消费者组id：NativeVisitList_1
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
      if (args != null && args.length == 3) {
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
          Subscribe[String, String](Set(topics), kafkaParams, fromOffsets))

        // 默认消费者偏移消费数据
      } else {

        KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](Set(topics, topics_test), kafkaParams))
      }
    //        .persist(StorageLevel.MEMORY_AND_DISK)
    /**
     * **************************************************************************
     * 理财师埋点处理流-----------------------------------------------------START--
     * **************************************************************************
     */
    // 定义v1_element_content内容
    val contents01 = List(
      "直播大厅_精彩回放_精彩回放列表",
      "发现_视频直播_常态视频区域",
      "首页_大家都在看_查看详情",
      "资讯_视频_大家都在看_视频",
      "资讯_点击视频tab",
      "首页_视频直播_内容模块",
      "快讯详情页访问",
      "视频页面离开_播放时长",
      "个股_个股详情页访问"
    )
    val contents02 = List(
      "视频直播页访问"
    )
    val contents03 = List(
      "视频直播播放"
    )
    val contents01Bro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contents01)

    // 5.处理数据:理财师埋点
    // 埋点数据Json解析,包含测试环境和线上环境所有数据，只取event、properties、time、project字段，因多次复用所以持久化
    val lcsEvent = dstream
      .map(record => {
        val dataMap = jsonParse(record.value())
        (dataMap.getOrElse("event", ""), dataMap.getOrElse("properties", ""), dataMap.getOrElse("time", ""), dataMap.getOrElse("project", ""))
      }).persist(StorageLevel.MEMORY_AND_DISK)


    val value = lcsEvent
      .filter(v => v._4 == "licaishi" && (v._1 == "LiveVisit" || v._1 == "NativeAppVisit" || v._1 == "NativeAppLeave" || v._1 == "NativeAppClick"))
      .map(v => {
        val properties = jsonParse(v._2)
        (properties.getOrElse("v1_element_content", ""),
          properties.getOrElse("v1_message_title", ""),
          properties.getOrElse("v1_message_id", ""),
          properties.getOrElse("v1_lcs_id", ""),
          properties.getOrElse("v1_lcs_name", ""),
          properties.getOrElse("userID", ""),
          v._3,
          properties.getOrElse("v1_symbol", ""),
          properties.getOrElse("v1_custom_params", "")
        )
      }).filter(v => contents01Bro.value.contains(v._1) || v._1 == "视频直播页访问" || v._1 == "视频直播播放")
      .filter(v => v._9 != "直播中" && v._9 != "直播" && v._9 != "视频直播") // 过滤掉v1_custom_params为这三种值得数据
    /**
     * {"type":"globalnews","uid":"11111","title":"标题","content":"内容，没有就不需要写","time":"2020-02-02 11:11:11","stay_time":"100"}
     * type:
     * 快讯:globalnews
     * 直播 enter_live  leave_live
     * 视频 enter_video leave_video
     */
    // 页面访问数据
    val pageVisit = value.filter(v => contents01Bro.value.contains(v._1))
      .map(row => {
        val content = row._1
        var `type` = ""
        if (content == "快讯详情页访问") {
          `type` = "globalnews"
        } else if (content == "视频页面离开_播放时长") {
          `type` = "leave_video"
        } else if (content == "个股_个股详情页访问") {
          `type` = "symbol"
        } else {
          `type` = "enter_video"
        }
        //(type,uid,title,content,message_id,lcs_id,lcs_name,time,stay_time,symbol)
        (`type`, row._6, row._2, content, row._3, row._4, row._5, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(row._7.toLong), 0, row._8)
      })

    // 直播间访问数据
    val liveVisit = value.filter(v => v._1 == "视频直播页访问" || v._1 == "视频直播播放")
      .map(row => {
        val content = row._1
        var `type` = ""
        if (content == "视频直播页访问") {
          `type` = "enter_live"
        } else if (content == "视频直播播放") {
          `type` = "on_video"
        }
        //((uid,lcs_id),(type,uid,title,content,message_id,lcs_id,lcs_name,time,stay_time))
        ((row._6, row._4), (`type`, row._6, row._2, content, row._3, row._4, row._5, row._7.toLong, 0))
      }).filter(v => v._2._3 != "") //过滤掉视频直播间title为空的数据，·视频直播页访问·title未埋点
      .updateStateByKey[(String, String, String, String, String, String, String, Long, Int)](updateFunc)
      .filter(v => v._2._1 == "enter_live" || v._2._1 == "leave_live")
      .map(v => (v._2._1, v._2._2, v._2._3, v._2._4, v._2._5, v._2._6, v._2._7, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(v._2._8), v._2._9, ""))

    //      pageVisit.union(liveVisit)
    //        .filter( v=> v._2 == "27271830" || v._2 == "27221768").print()
    // 结果数据
    pageVisit.union(liveVisit)
      .foreachRDD(lines => {

        //存储到hive
        //          val result = lines.map(v => Row(v._1,v._2,v._3,v._4,v._5,v._6,v._7,v._8,v._9,v._10))
        //          val structType = StructType(Array(
        //            StructField("type", StringType, true),
        //            StructField("uid", StringType, true),
        //            StructField("title", StringType, true),
        //            StructField("content", StringType, true),
        //            StructField("message_id", StringType, true),
        //            StructField("lcs_id", StringType, true),
        //            StructField("lcs_name", StringType, true),
        //            StructField("time", StringType, true),
        //            StructField("stay_time", IntegerType, true),
        //            StructField("symbol", StringType, true)
        //          ))
        //
        //          val sqlContext: SQLContext = new HiveContext(lines.sparkContext)
        //          //        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        //          //        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        //          val valueDf: DataFrame = sqlContext.createDataFrame(result, structType)
        //          valueDf.createOrReplaceGlobalTempView("t")
        //          sqlContext.sql("insert into lcs_test.dwd_buried_event_list select * from global_temp.t")

        //存储到redis
        lines.foreachPartition(rdd => {
          val jedis = RedisClient.pool.getResource
          rdd.foreach(v => {
            val jsonObj = new JSONObject
            jsonObj.put("type", v._1)
            jsonObj.put("uid", v._2)
            jsonObj.put("title", v._3)
            jsonObj.put("content", v._4)
            jsonObj.put("message_id", v._5)
            jsonObj.put("lcs_id", v._6)
            jsonObj.put("lcs_name", v._7)
            jsonObj.put("time", v._8)
            jsonObj.put("stay_time", new Integer(v._9))
            jsonObj.put("symbol", v._10)
            //      jedis.select(1)
            jedis.rpush("lcs:buried:event:list", jsonObj.toJSONString)
          })
          jedis.close()
        })
      })

    /**
     * **************************************************************************
     * 理财师埋点处理流-------------------------------------------------------END--
     * **************************************************************************
     */
    /**
     * **************************************************************************
     * TD公众号埋点处理流---------------------------------------------------START--
     * **************************************************************************
     */
    // 定义v1_element_content内容
    val contentsTD = List(
      "金股天下_菜单栏点击",
      "金股天下_推送点击",
      "金股天下_文章点击",
      "金股天下_文章列表访问",
      "金股天下_文章访问"
    )
    val contentsTDBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsTD)
    // 5.处理数据：TD公众号埋点
    val valueTD = lcsEvent
      .filter(v => v._4 == "licaishi" && (v._1 == "TDGZHVisit" || v._1 == "TDGZHClick" || v._1 == "TDVisit" || v._1 == "TDClick"))
      .map(v => {
        val properties = jsonParse(v._2)
        (
          properties.getOrElse("wx_uid", ""),
          properties.getOrElse("v1_element_content", ""),
          properties.getOrElse("v1_message_title", ""),
          properties.getOrElse("v1_message_type", ""),
          properties.getOrElse("v1_custom_params", ""),
          properties.getOrElse("v1_message_id", ""),
          v._3
        )
      }).filter(v => contentsTDBro.value.contains(v._2) && v._1 != "" && v._1 != null).persist(StorageLevel.MEMORY_AND_DISK)

    /**
     * TD 项目
     *1.用户行为明细redis list , key:   lcs:td:buried:event:list
     * {"uid":"11111","type":"","content":"金股天下_菜单栏点击","title":"标题","msg_type":"0","tuisong_param":"全部推送",time":"2020-02-02 11:11:11"}
     * type:
     * menu:"金股天下_菜单栏点击",
     * push:"金股天下_推送点击",
     * artiClick:"金股天下_文章点击",
     * artiList:"金股天下_文章列表访问"
     * artiVisit:"金股天下_文章访问"
     * msg_type：消息（0），文本（1），图片（2）
     * tuisong_param（限金股天下_推送点击）：全部推送,签到推送
     * *
     *2.文章阅读数redis hash , key:   lcs:td:buried:read:num
     * (key：文章id，value：阅读数)
     *
     */
    // 行为记录数据
    //      valueTD.map(row => (row._1,row._2,row._3,row._4,row._5,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(row._7.toLong)))
    //        .foreachRDD(lines => {
    //          //存储到redis
    //          lines.foreachPartition( rdd => {
    //            val jedis = RedisClient.pool.getResource
    //            rdd.foreach(v =>{
    //              val jsonObj = new JSONObject
    //              jsonObj.put("uid",v._1)
    //              if (v._2 == "金股天下_菜单栏点击")
    //                jsonObj.put("type","menu")
    //              else if (v._2 == "金股天下_推送点击")
    //                jsonObj.put("type","push")
    //              else if (v._2 == "金股天下_文章点击")
    //                jsonObj.put("type","artiClick")
    //              else if (v._2 == "金股天下_文章列表访问")
    //                jsonObj.put("type","artiList")
    //              else if (v._2 == "金股天下_文章访问")
    //                jsonObj.put("type","artiVisit")
    //              jsonObj.put("content", v._2)
    //              jsonObj.put("title",v._3)
    //              jsonObj.put("msg_type", v._4)
    //              jsonObj.put("tuisong_param",v._5)
    //              jsonObj.put("time", v._6)
    //              //      jedis.select(1)
    ////              println("**********行为记录："+jsonObj.toJSONString)
    //              jedis.rpush("lcs:td:buried:event:list",jsonObj.toJSONString)
    //            })
    //            jedis.close()
    //          })
    //        })

    // 文章阅读数统计
    valueTD.filter(v => v._2 == "金股天下_文章访问")
      .map(row => (row._6, (row._1, row._7)))
      .updateStateByKey[(mutable.Map[String, String], Long, String)](updateFuncTD) //类型为key=文章id,value=(Map(用户id,时间),阅读数,最新时间)
      .filter(v => {
        v._2._2 > 0
      }).map(v => (v._1, v._2._2))
      .foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            val jedis = RedisClient.pool.getResource

            list.foreach(r => {
              val str = jedis.hget("lcs:td:buried:read:num", r._1)
              val num = if (str == "null" || str == null) 0 else str.toInt
              //                  println("**********阅读统计：" + r._1 + "原次数:" +num)
              //                  println("**********阅读统计：" + r._1 + "加次数:" +r._2)
              jedis.hset("lcs:td:buried:read:num", r._1, (r._2 + num).toString)
            })
            jedis.close()
          }
        })
      })

    /**
     * **************************************************************************
     * TD公众号埋点处理流-----------------------------------------------------END--
     * **************************************************************************
     */


    /**
     * **************************************************************************
     * 视频、动态、新闻播放量统计--------------------------------------------Start--
     * **************************************************************************
     */
    // 6.处理数据：视频、动态、新闻播放量记录埋点
    val contentsCount = List(
      "视频_播放页面访问",
      "动态详情页访问"
    )
    val contentsCountBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsCount)
    lcsEvent
      .filter(v => v._1 == "NativeAppVisit" && v._4 == "licaishi")
      .map(v => {
        val properties = jsonParse(v._2)
        (
          properties.getOrElse("v1_element_content", ""),
          properties.getOrElse("v1_message_title", ""),
          properties.getOrElse("v1_message_type", ""),
          properties.getOrElse("v1_custom_params", ""),
          properties.getOrElse("v1_message_id", ""),
          v._3
        )
      }).filter(v => contentsCountBro.value.contains(v._1) && v._5 != "" && v._5 != null)
      .map(v => (
        v._1 match {
          case "视频_播放页面访问" => "vedio"
          case "动态详情页访问" => "dynamic"
        }, v._5))
      .countByValueAndWindow(Seconds(60 * 10), Seconds(60 * 10))
      .foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            val jedis = RedisClient.pool.getResource
            list.foreach(v => {
              val jsonObj = new JSONObject
              jsonObj.put("type", v._1._1)
              jsonObj.put("msg_id", v._1._2)
              jsonObj.put("num", v._2 + "")
              jedis.rpush("lcs:buried:count:list", jsonObj.toJSONString)
            })
            jedis.close()
          }
        })
      })

    /**
     * **************************************************************************
     * 视频、动态、新闻播放量统计----------------------------------------------End--
     * **************************************************************************
     */

    /**
     * **************************************************************************
     * 观点、动态、视频分享统计  --------------------------------------------Start--
     * **************************************************************************
     */
    // 7.观点、动态、视频分享记录埋点
    val shareContentsCount = List(
      "观点详情页_页中分享按钮",
      "观点详情页_底部栏_分享按钮",
      "动态详情页_底部栏_分享按钮",
      "视频播放页面_分享按钮"
    )
    val shareContentsCountBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(shareContentsCount)
    val shareContents = lcsEvent
      .filter(v => (v._1 == "NativeAppClick" || v._1 == "H5Click") && v._4 == "licaishi")
      .map(v => {
        val properties = jsonParse(v._2)
        (
          properties.getOrElse("v1_element_content", ""),
          properties.getOrElse("v1_message_title", ""),
          properties.getOrElse("v1_message_type", ""),
          properties.getOrElse("v1_custom_params", ""),
          properties.getOrElse("v1_message_id", ""),
          properties.getOrElse("v1_lcs_id", ""),
          v._3
        )
      }).persist(StorageLevel.MEMORY_AND_DISK)
    // 理财师维度统计
    shareContents.filter(v => shareContentsCountBro.value.contains(v._1) && v._6 != "" && v._6 != null)
      .map(v => (
        v._1 match {
          case "观点详情页_页中分享按钮" => "view_share_num"
          case "观点详情页_底部栏_分享按钮" => "view_share_num"
          case "动态详情页_底部栏_分享按钮" => "dynamic_share_num"
          case "视频播放页面_分享按钮" => "video_share_num"
        }, v._6, new SimpleDateFormat("yyyy-MM-dd").format(v._7.toLong)))
      .countByValueAndWindow(Seconds(60 * 1), Seconds(60 * 1))
      .foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            val jedis = RedisClient.pool.getResource
            list.foreach(v => {
              val key = "lcs:planner:stat:" + v._1._3 + ":" + v._1._2
              val num = jedis.hget(key, v._1._1)
              val newNum = (if (null != num) num.toLong else 0L) + v._2
              jedis.hset(key, v._1._1, newNum.toString)
            })
            jedis.close()
          }
        })
      })
    // message_id维度统计
    shareContents.filter(v => shareContentsCountBro.value.contains(v._1) && v._5 != "" && v._5 != null)
      .map(v => (v._1, v._5))
      .countByValueAndWindow(Seconds(60 * 1), Seconds(60 * 1))
      .foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            val jedis = RedisClient.pool.getResource
            list.foreach(v => {
              val key = "lcs:planner:stat:share_num"
              val shareStr = v._1._1 match {
                case "观点详情页_页中分享按钮" => "view_share_"
                case "观点详情页_底部栏_分享按钮" => "view_share_"
                case "动态详情页_底部栏_分享按钮" => "dynamic_share_"
                case "视频播放页面_分享按钮" => "video_share_"
              }
              val num = jedis.hget(key, shareStr + v._1._2)
              val newNum = (if (null != num) num.toLong else 0L) + v._2
              jedis.hset(key, shareStr + v._1._2, newNum.toString)
            })
            jedis.close()
          }
        })
      })

    /**
     * **************************************************************************
     * 观点、动态、视频分享统计----------------------------------------------End--
     * **************************************************************************
     */

    /**
     * **************************************************************************
     * 推送启动app统计--------------------------------------------Start--
     * **************************************************************************
     */

    // 定义v1_element_content内容
    val contentsPushStrartApp = List(
      "推送启动app"
    )
    val contentsPushStrartAppBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsPushStrartApp)
    lcsEvent
      .filter(v => v._1 == "LCSAppStart")
      .map(v => {
        val properties = jsonParse(v._2)
        (
          properties.getOrElse("v1_element_content", ""),
          properties.getOrElse("v1_notice_id", "")
        )
      }).filter(v => contentsPushStrartAppBro.value.contains(v._1) && v._2 != "" && v._2 != "0" && v._2 != "(null)")
      .foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            val jedis = RedisClient.pool.getResource
            list.foreach(r => {

              val str = jedis.hget("lcs:buried:pushStartApp:num", r._2)
              val num = if (str == "null" || str == null) 0 else str.toInt
              jedis.hset("lcs:buried:pushStartApp:num", r._2, (num + 1).toString)

            })
            jedis.close()
          }
        })
      })

    /**
     * **************************************************************************
     * 推送启动app统计----------------------------------------------End--
     * **************************************************************************
     */

    /**
     * **************************************************************************
     * CSH5Visit事件部分数据引流到redis list-----------------------------------Start--
     * **************************************************************************
     */

    // 定义v1_element_content内容
    val contentsCSH5 = List(
      "学籍注册页访问",
      "学籍注册页_学号页访问",
      "问卷填写页访问",
      "问卷完成页访问",
      "课程详情页访问",
      "课程内容页访问",
      "推广营销页访问",
      "课程详情页_购买",
      "课程详情页_章节",
      "课程详情页_tab",
      "视频直播_弹幕",
      "视频直播_点赞",
      "视频直播_悬浮广告"
    )
    val contentsCSH5Bro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsCSH5)

    val contentsCSH5Lcs = List(
      "课程详情页访问",
      "课程详情页_购买"
    )
    val contentsCSH5LcsBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsCSH5Lcs)


    lcsEvent
      // 测试环境数据
      .filter(v => (v._1 == "CSH5Visit" || v._1 == "CSH5Click") && v._4 == "lcs_test")
      .foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            val jedis = RedisClient.pool.getResource
            list.foreach(v => {
              val jsonParser = new JSONParser()
              val value = jsonParser.parse(v._2).asInstanceOf[JSONObject]
              value.put("event", v._1)
              value.put("time", v._3)
              val v1_element_content = value.getOrDefault("v1_element_content", "")
              if (contentsCSH5Bro.value.contains(v1_element_content)) {
                jedis.rpush("bx:behevior:list:caishang:test", value.toJSONString())
              }
              if (contentsCSH5LcsBro.value.contains(v1_element_content)) {
                jedis.rpush("lcs:caishang:user_option:dev", value.toJSONString())
              }
            })
            jedis.close()
          }
        })
      })

    lcsEvent
      // 线上环境数据
      .filter(v => (v._1 == "CSH5Visit" || v._1 == "CSH5Click") && v._4 == "licaishi")
      .foreachRDD(lines => {
        //存储到redis
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            val jedis = RedisClient.pool.getResource
            list.foreach(v => {
              val jsonParser = new JSONParser()
              val value = jsonParser.parse(v._2).asInstanceOf[JSONObject]
              value.put("event", v._1)
              value.put("time", v._3)
              val v1_element_content = value.getOrDefault("v1_element_content", "")
              if (contentsCSH5Bro.value.contains(v1_element_content)) {
                jedis.rpush("bx:behevior:list:caishang", value.toJSONString())
              }
              if (contentsCSH5LcsBro.value.contains(v1_element_content)) {
                jedis.rpush("lcs:caishang:user_option:prod", value.toJSONString())
              }
            })
            jedis.close()
          }
        })
      })

    /**
     * **************************************************************************
     * CSH5Visit事件部分数据引流到redis list----------------------------------End--
     * **************************************************************************
     */

    /**
     * **************************************************************************
     * 财商课程章节用户观看进度统计-------------------------------------------Start--
     * **************************************************************************
     */
    val caishangCourseViewRecord = lcsEvent
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


    caishangCourseViewRecord.foreachRDD(lines => {
      //存储到HBase
      lines.foreachPartition(rdd => {
        val list = rdd.toList
        if (list.nonEmpty) {
          // hbase连接初始化
          val conn = HbaseUtilsScala.getConnection
          val table = TableName.valueOf("caishang_course_viewing_record")
          val htable = conn.getTable(table)
          list.foreach(v => {
            if (v._1 == "财商_课程_直播") {
              val timestamp = v._6.toLong / 1000 / 30 * 1000 * 30
              if (v._7 == "licaishi") {
                HbaseUtilsScala.setRow(htable, v._5.reverse + "|" + v._3 + "|prod|live|" + timestamp, "cf1", "record", "1")
              } else if (v._7 == "lcs_test") {
                HbaseUtilsScala.setRow(htable, v._5.reverse + "|" + v._3 + "|test|live|" + timestamp, "cf1", "record", "1")
              }
            } else if (v._1 == "财商_课程_回看") {
              if (v._7 == "licaishi") {
                HbaseUtilsScala.setRow(htable, v._5.reverse + "|" + v._3 + "|prod|play|" + v._4, "cf1", "record", "1")
              } else if (v._7 == "lcs_test") {
                HbaseUtilsScala.setRow(htable, v._5.reverse + "|" + v._3 + "|test|play|" + v._4, "cf1", "record", "1")
              }
            }
          })

        }
      })
    })
    // 测试&线上环境数据一起处理
    // 取uid、课程章节id、埋点环境，
    caishangCourseViewRecord.map(v => ((v._5, v._3, v._7), 1)).reduceByKeyAndWindow((v1, v2) => v1, Seconds(30))
      .foreachRDD(lines => {
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            // hbase连接初始化
            val conn = HbaseUtilsScala.getConnection
            val table = TableName.valueOf("caishang_course_viewing_record")
            val htable = conn.getTable(table)

            // 初始化mysql连接
            Class.forName("com.mysql.jdbc.Driver").newInstance()
            val connection = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "syl_w", "naAm7kmYgaG7SrkO1mAT")
            val connectionTest = DriverManager.getConnection("jdbc:mysql://rm-2ze65101j46337ry1.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "licaishi_w", "tUG78nfG6XottQGboRpR")
            val stmt = connection.createStatement
            val stmtTest = connectionTest.createStatement
            try {
              list.distinct.foreach(v => {
                var stmtOn: Statement = null
                val uid = v._1._1
                val courseItemId = v._1._2
                var startTime = "0000-00-00 00:00:00"
                var endTime = "0000-00-00 00:00:00"
                var env = ""
                if (v._1._3 == "licaishi") { //线上数据
                  stmtOn = stmt
                  env = "prod"
                } else if (v._1._3 == "lcs_test") { //测试数据
                  stmtOn = stmtTest
                  env = "test"
                }

                val sql1 =
                  s"""
                     |SELECT
                     |	b.start_time,
                     |	b.end_time
                     |FROM
                     |	( SELECT circle_notice_id FROM `wealth_business_course_item` WHERE id = $courseItemId ) a
                     |	LEFT JOIN lcs_circle_notice b ON a.circle_notice_id = b.id
                     |""".stripMargin
                val rs: ResultSet = stmtOn.executeQuery(sql1)
                while (rs.next()) {
                  startTime = rs.getString("start_time")
                  endTime = rs.getString("end_time")
                }
                rs.close()

                val hbaseRecords = scaneByPrefixFilter(htable, uid.reverse + "|" + courseItemId + "|" + env)
                val tuples = hbaseRecords.map(v => {
                  val splits = v.split("\\|")
                  (splits(3), splits(4))
                })
                val halfMinutes = getBetweenHalfMinute(startTime, endTime)
                val live = tuples.filter(_._1.equals("live")).map(v => halfMinutes.indexOf(v._2)).filter(_ != -1)
                val play = tuples.filter(_._1.equals("play")).map(_._2.toInt / 30)
                val proceding = live.union(play).toSet
                val halfMinutesPoint = halfMinutes.indices

                val intersect = halfMinutesPoint.toSet.intersect(proceding) // 交集，以观看
                val diff = halfMinutesPoint.toSet.diff(proceding) // 差集，未观看

                val percent = intersect.size * 1.0 / halfMinutesPoint.size
                val times = intersect.size * 30

                val jsonObj = new JSONObject
                jsonObj.put("percent", percent + "")
                jsonObj.put("times", times + "")
                val jsonObjStr = jsonObj.toJSONString()

                val sql2 =
                  s"""
                     |UPDATE wealth_business_user_view_course_history
                     |SET position = '$jsonObjStr'
                     |WHERE
                     |	uid = $uid
                     |	AND course_item_id = $courseItemId
                     |""".stripMargin

                stmtOn.execute(sql2)

              })
            } catch {
              case e: Exception => e.printStackTrace()
            } finally {
              stmt.close()
              stmtTest.close()
              connection.close()
              connectionTest.close()
              conn.close() //hbase 关闭连接
            }
          }

        })
      })

    /**
     * **************************************************************************
     * 财商课程章节用户观看进度统计--------------------------------------------End--
     * **************************************************************************
     */

    /**
     * **************************************************************************
     * 财商作业答题用户行为--------------------------------------------------Start--
     * **************************************************************************
     */
    // 定义v1_element_content内容
    val contentsCSAnswerQuestionClick = List(
      "财商_题目解答页_选项点击",
      "财商_题目解答页_提交点击",
      "财商_题目解答页_回看课程点击",
      "财商_评分结果页_错题解析",
      "财商_评分结果页_全部解析",
      "财商_评分结果页_学习其他课程",
      "财商_评分结果页_我要打卡",
      "财商_评分结果页_重新答题"
    )
    val contentsCSAnswerQuestionClickBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsCSAnswerQuestionClick)

    val contentsCSAnswerQuestionVisit = List(
      "财商_题目解答页访问",
      "财商_评分结果页访问",
      "财商_图片打卡页访问"
    )
    val contentsCSAnswerQuestionVisitBro: Broadcast[List[String]] = ssc.sparkContext.broadcast(contentsCSAnswerQuestionVisit)


    val caishangAnswerQuestion = lcsEvent
      .filter(v => (v._1 == "CSH5Visit" || v._1 == "CSH5Click"))
      .map(v => {
        val properties = jsonParse(v._2)
        ( v._3, // 时间戳
          v._4, // project，区分测试和线上环境
          properties.getOrElse("v1_element_content", ""),
          properties.getOrElse("v1_message_title", ""),
          properties.getOrElse("v1_message_id", ""),
          properties.getOrElse("v1_page_title", ""),
          properties.getOrElse("v1_uid", ""),
          properties.getOrElse("v1_remark", ""),
          properties.getOrElse("v1_action", ""),
          properties.getOrElse("v1_custom_params", "")

        )
      })
      .filter(v => contentsCSAnswerQuestionClickBro.value.contains(v._3) || contentsCSAnswerQuestionVisitBro.value.contains(v._3))
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 测试&线上环境数据一起处理
    caishangAnswerQuestion
      .foreachRDD(lines => {
        lines.foreachPartition(rdd => {
          val list = rdd.toList
          if (list.nonEmpty) {
            // 初始化mysql连接
            Class.forName("com.mysql.jdbc.Driver").newInstance()
            val connection = DriverManager.getConnection("jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "syl_w", "naAm7kmYgaG7SrkO1mAT")
            val connectionTest = DriverManager.getConnection("jdbc:mysql://rm-2ze65101j46337ry1.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "licaishi_w", "tUG78nfG6XottQGboRpR")
            val stmt = connection.createStatement
            val stmtTest = connectionTest.createStatement
            try {
              list.distinct.foreach(v => {
                var stmtOn: Statement = null
                if (v._2 == "licaishi") { //线上数据
                  stmtOn = stmt
                } else if (v._2 == "lcs_test") { //测试数据
                  stmtOn = stmtTest
                }

                val messageTitle = v._4
                val messageId = v._5
                val pageTitle = v._6
                val uid = v._7
                val remark = v._8
                val action = v._9
                var typeStr = ""

                if (contentsCSAnswerQuestionClickBro.value.contains(v._3)) {
                  typeStr = "click"
                } else if (contentsCSAnswerQuestionVisitBro.value.contains(v._3)) {
                  typeStr = "enter"
                  val sql2 =
                    s"""
                       |SELECT ext_info FROM lcs_wealth_ques_user WHERE ques_id = $messageId and uid = $uid
                       |""".stripMargin
                  val rs: ResultSet = stmtOn.executeQuery(sql2)
                  var ext_info = "{}"
                  while (rs.next()) {
                    ext_info = rs.getString("ext_info")
                  }
                  if (ext_info == "") ext_info = "{}"
                  rs.close()

                  val jsonParser = new JSONParser()
                  val value = jsonParser.parse(ext_info).asInstanceOf[JSONObject]
                  if(v._3 == "财商_题目解答页访问") {
                    value.put("enter_answer_page", Integer.valueOf(1))
                  }
                  if(v._3 == "财商_评分结果页访问") {
                    value.put("enter_score_page", Integer.valueOf(1))
                  }
                  if(v._3 == "财商_图片打卡页访问") {
                    value.put("enter_course_sign_page", Integer.valueOf(1))
                  }

                  val sql3 =
                    s"""
                       |UPDATE lcs_wealth_ques_user
                       |SET ext_info = '${value.toJSONString()}'
                       |WHERE
                       |	ques_id = $messageId AND uid = $uid
                       |""".stripMargin

                  val sql4 =
                    s"""
                       | INSERT INTO lcs_wealth_ques_user (ques_id , uid ,ext_info ,ques_relation_type )
                       | SELECT $messageId,$uid,'${value.toJSONString()}',1
                       |FROM
                       |	DUAL
                       |WHERE
                       |	NOT EXISTS (
                       |		SELECT
                       |			ques_id
                       |		FROM
                       |			lcs_wealth_ques_user
                       |		WHERE
                       |			ques_id = $messageId AND uid = $uid
                       |	);
                       |""".stripMargin

                  stmtOn.execute(sql3)
                  stmtOn.execute(sql4)
                }

                val sql1 =
                  s"""
                     |INSERT INTO wealth_business_user_history ( uid , category , relation_id , type , remark ,page_title ,message_title ,action)
                     | VALUES
                     |  ( $uid, 'answer_question', $messageId , '$typeStr' , '$remark' , '$pageTitle' , '$messageTitle' , '$action');
                     |""".stripMargin
                stmtOn.execute(sql1)


              })
            } catch {
              case e: Exception => e.printStackTrace()
            } finally {
              stmt.close()
              stmtTest.close()
              connection.close()
              connectionTest.close()
            }
          }
        })
      })
    /**
     * **************************************************************************
     * 财商作业答题用户行为----------------------------------------------------End--
     * **************************************************************************
     */


    // 9.开启流式计算
    ssc.start()

    // 阻塞一直运行
    ssc.awaitTermination()

  }

  val updateFunc = (currValues: Seq[(String, String, String, String, String, String, String, Long, Int)],
                    prevValueState: Option[(String, String, String, String, String, String, String, Long, Int)]) => {
    // 通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    // 已累加的值
    // 返回累加后的结果。是一个Option[T]类型
    val prev = prevValueState.getOrElse(("leave_live", "", "", "", "", "", "", 0L, 0))
    val enters = currValues.filter(_._1 == "enter_live")
    val ons = currValues.filter(_._1 == "on_video")
    var result = ("", "", "", "", "", "", "", 0L, 0)

    if (prev._1 == "leave_live" || prev._1 == "") {
      //上次状态为空或"leave_live"，此次状态为进入
      if (enters.nonEmpty) { // 单批次内有多条数据，停留时间初始值取最大最小值之差
        val tempmin = currValues.minBy(_._8)
        val tempMax = currValues.maxBy(_._8)
        result = ("enter_live", tempMax._2, tempMax._3, tempMax._4, tempMax._5, tempMax._6, tempMax._7, tempMax._8, (tempMax._8 - tempmin._8).toInt / 1000)
      } else if (ons.nonEmpty) { // 单批次内有多条数据，停留时间初始值取最大最小值之差
        val temp = ons.minBy(_._8)
        val tempMax = ons.maxBy(_._8)
        result = ("enter_live", temp._2, temp._3, temp._4, temp._5, temp._6, temp._7, temp._8, (tempMax._8 - temp._8).toInt / 1000)
      }
    } //上次状态为"enter_live"，此次状态更新时间戳
    else if (prev._1 == "enter_live" || prev._1 == "on_live") {
      if (ons.nonEmpty) {
        val temp = ons.maxBy(_._8)
        result = ("on_live", temp._2, temp._3, temp._4, temp._5, temp._6, temp._7, temp._8, Math.abs(temp._8 - prev._8).toInt / 1000 + prev._9)
      } else if (enters.isEmpty) {
        result = ("leave_live", prev._2, prev._3, prev._4, prev._5, prev._6, prev._7, prev._8 + 3000, prev._9 + 3)
      }
    }
    Some(result)
  }
  val updateFuncTD: (Seq[(String, String)], Option[(mutable.Map[String, String], Long, String)]) => Some[(mutable.Map[String, String], Long, String)] = (currValues: Seq[(String, String)],
                                                                                                                                                         prevValueState: Option[(mutable.Map[String, String], Long, String)]) => {
    // 通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    // 已累加的值
    // 返回累加后的结果。是一个Option[T]类型
    var currMap = currValues.groupBy(_._1).map(v => (v._1, v._2.maxBy(_._2)._2))
    val userMap = prevValueState.getOrElse((mutable.Map[String, String](), 0L, "0"))
    var map = userMap._1 // 之前userMap
    var addOne = 0 // 阅读数清零
    var time = userMap._3 // 之前时间戳
    var maxTime = "0" // 该批次的最大时间戳
    if (currMap.nonEmpty) {
      maxTime = currMap.maxBy(_._2)._2 // 该批次的最大时间戳
    }

    currMap.map(v => {

      try {
        if (!map.contains(v._1)) { // 之前数据无该uid，+1
          addOne += 1
        } else if ((v._2.toLong - map.getOrElse(v._1, "0").toLong) > 100000) { // 之前阅读数据时间与当前阅读时间之差大于100秒，+1
          addOne += 1
          map += (v._1 -> v._2)
        } else {
          currMap += (v._1 -> map.getOrElse(v._1, "0"))
        }
      } catch {
        case ex: Exception => {

        }
      }
      time = maxTime
      map = map ++ currMap
      map.filter(v => (maxTime.toLong - v._2.toLong) > 120000)

    })

    Some((map, addOne, time))
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
                     v1_tg_open_id: String,
                     sc_comments: String,
                     sport_name: String,
                     v1_custom_params2: String,
                     infor_list: String,
                     dt_commit_time: String
                   )

}
