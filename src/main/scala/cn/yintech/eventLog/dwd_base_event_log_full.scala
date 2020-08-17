package cn.yintech.eventLog

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.util.matching.Regex

object dwd_base_event_log_full {

  def main(args: Array[String]): Unit = {

    //  获取日期分区参数
    require(!(args == null || args.length == 0 || args(0) == ""), "Required 'dt' arg")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome = pattern findFirstIn args(0)
    require(dateSome.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    val dt = dateSome.get // 实际使用yyyy-mm-dd格式日期
    println("update dt : " + dt)
//    val dt = "2020-02-27"

    //saprk切入点
    val spark = SparkSession.builder()
      .appName("dwd_event_log")
      .enableHiveSupport()
      .getOrCreate()

      import spark.implicits._

      val data = spark.sql(s"select line from lcs_test.ods_event_log where dt = '$dt' ")
      val value = data
        .map(row => {
        val line = if (row(0) == null ) "" else row.getString(0)
        val dataMap = jsonParse(line)
        val eventLog =  EventLogBean(
          dataMap.getOrElse("ver",""),
          dataMap.getOrElse("recv_time",""),
          dataMap.getOrElse("lib_lib",""),
          dataMap.getOrElse("lib_lib_method",""),
          dataMap.getOrElse("lib_lib_version",""),
          dataMap.getOrElse("lib_app_version",""),
          dataMap.getOrElse("lib_lib_detail",""),
          dataMap.getOrElse("extractor_s",""),
          dataMap.getOrElse("extractor_c",""),
          dataMap.getOrElse("extractor_e",""),
          dataMap.getOrElse("extractor_f",""),
          dataMap.getOrElse("extractor_n",""),
          dataMap.getOrElse("extractor_o",""),
          dataMap.getOrElse("project",""),
          dataMap.getOrElse("tmp_hybrid_h5",""),
          dataMap.getOrElse("type",""),
          dataMap.getOrElse("tmp_track_id",""),
          dataMap.getOrElse("map_id",""),
          dataMap.getOrElse("user_id",""),
          dataMap.getOrElse("project_id",""),
          dataMap.getOrElse("distinct_id",""),
          dataMap.getOrElse("tmp_flush_time",""),
          dataMap.getOrElse("time",""),
          dataMap.getOrElse("event",""),
          dataMap.getOrElse("properties_os",""),
          dataMap.getOrElse("properties_is_first_time",""),
          dataMap.getOrElse("properties_network_type",""),
          dataMap.getOrElse("properties_wifi",""),
          dataMap.getOrElse("properties_ip",""),
          dataMap.getOrElse("properties_screen_height",""),
          dataMap.getOrElse("properties_referrer",""),
          dataMap.getOrElse("properties_deviceId",""),
          dataMap.getOrElse("properties_userID",""),
          dataMap.getOrElse("properties_PushStatus",""),
          dataMap.getOrElse("properties_url_path",""),
          dataMap.getOrElse("properties_device_id",""),
          dataMap.getOrElse("properties_planner_number",""),
          dataMap.getOrElse("properties_latest_search_keyword",""),
          dataMap.getOrElse("properties_province",""),
          dataMap.getOrElse("properties_url",""),
          dataMap.getOrElse("properties_latest_referrer",""),
          dataMap.getOrElse("properties_carrier",""),
          dataMap.getOrElse("properties_os_version",""),
          dataMap.getOrElse("properties_referrer_host",""),
          dataMap.getOrElse("properties_is_first_day",""),
          dataMap.getOrElse("properties_city",""),
          dataMap.getOrElse("properties_model",""),
          dataMap.getOrElse("properties_screen_width",""),
          dataMap.getOrElse("properties_utm_source",""),
          dataMap.getOrElse("properties_app_version",""),
          dataMap.getOrElse("properties_lib",""),
          dataMap.getOrElse("properties_stock_number",""),
          dataMap.getOrElse("properties_country",""),
          dataMap.getOrElse("properties_utm_campaign",""),
          dataMap.getOrElse("properties_title",""),
          dataMap.getOrElse("properties_lib_version",""),
          dataMap.getOrElse("properties_latest_traffic_source_type",""),
          dataMap.getOrElse("properties_PlatformType",""),
          dataMap.getOrElse("properties_latest_referrer_host",""),
          dataMap.getOrElse("properties_platForm",""),
          dataMap.getOrElse("properties_manufacturer",""),
          dataMap.getOrElse("properties_is_login_id",""),
          dataMap.getOrElse("properties_v1_element_content",""),
          dataMap.getOrElse("properties_v1_message_title",""),
          dataMap.getOrElse("properties_v1_message_type",""),
          dataMap.getOrElse("properties_v1_symbol",""),
          dataMap.getOrElse("properties_v1_stock_name",""),
          dataMap.getOrElse("properties_v1_lcs_name",""),
          dataMap.getOrElse("properties_v1_lcs_id",""),
          dataMap.getOrElse("properties_v1_isfirst",""),
          dataMap.getOrElse("properties_v1_share_channel",""),
          dataMap.getOrElse("properties_v1_source",""),
          dataMap.getOrElse("properties_v1_order",""),
          dataMap.getOrElse("properties_v1_custom_params",""),
          dataMap.getOrElse("properties_v1_custom_params2",""),
          dataMap.getOrElse("properties_v1_environment",""),
          dataMap.getOrElse("properties_v1_page_url",""),
          dataMap.getOrElse("properties_v1_remain",""),
          dataMap.getOrElse("properties_v1_paly_time",""),
          dataMap.getOrElse("properties_v1_ispush",""),
          dataMap.getOrElse("properties_v1_ishot",""),
          dataMap.getOrElse("properties_v1_message_id","")
        )
        eventLog
      })
      //      value.show(10)

      value.createOrReplaceGlobalTempView("eventlog")

      spark.sql(s"insert into lcs_test.dwd_base_event_log partition(dt='$dt')" +
        " select * from global_temp.eventlog ")
//      .write
//      .mode(SaveMode.Append)
//      .format("Hive")
//      .partitionBy("dt")
//      .saveAsTable("lcs_test.dws_event_log")

    spark.stop()


  }

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()

    try{
      // json字符串不规范的话会抛异常ClassCastException
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        var outKey = outIter.next()
        var outValue = outJsonObj.get(outKey).toString

        try {
          // json字符串不规范的话会抛异常ClassCastException
          if (outKey.startsWith("_")) outKey = "tmp" + outKey
          val innerJsonObj: JSONObject = jsonParser.parse(outValue).asInstanceOf[JSONObject]
          val innerJsonKey = innerJsonObj.keySet()
          val innerIter = innerJsonKey.iterator
          while (innerIter.hasNext) {
            val innerKey = innerIter.next()
            var innerValue = innerJsonObj.get(innerKey).toString
            //          println("key: " + (outKey+"_"+innerKey).replace("$","") + " value:" + innerValue)
            if (innerValue.startsWith("$"))
              innerValue = innerValue.replaceFirst("$","")
            map += ((outKey + "_" + innerKey).replace("$", "") -> innerValue)
          }
        } catch {
          case ex: Exception => {
            //          System.err.println("exception===>: ...")  // 打印到标准err
            //          println("key: " + outKey + " value:" + outValue)
            if (outValue.startsWith("$"))
              outValue = outValue.replaceFirst("$","")
            map += (outKey -> outValue)
          }
        }
      }
    } catch {
      case ex: Exception => {

      }
    }
    map
  }

  case class EventLogBean (ver:String,recv_time:String,lib_lib:String,lib_lib_method:String,lib_lib_version:String,lib_app_version:String,lib_lib_detail:String,extractor_s:String,extractor_c:String,extractor_e:String,extractor_f:String,extractor_n:String,extractor_o:String,project:String,tmp_hybrid_h5:String,`type`:String,tmp_track_id:String,map_id:String,user_id:String,project_id:String,distinct_id:String,tmp_flush_time:String,time:String,event:String,properties_os:String,properties_is_first_time:String,properties_network_type:String,properties_wifi:String,properties_ip:String,properties_screen_height:String,properties_referrer:String,properties_deviceId:String,properties_userID:String,properties_PushStatus:String,properties_url_path:String,properties_device_id:String,properties_planner_number:String,properties_latest_search_keyword:String,properties_province:String,properties_url:String,properties_latest_referrer:String,properties_carrier:String,properties_os_version:String,properties_referrer_host:String,properties_is_first_day:String,properties_city:String,properties_model:String,properties_screen_width:String,properties_utm_source:String,properties_app_version:String,properties_lib:String,properties_stock_number:String,properties_country:String,properties_utm_campaign:String,properties_title:String,properties_lib_version:String,properties_latest_traffic_source_type:String,properties_PlatformType:String,properties_latest_referrer_host:String,properties_platForm:String,properties_manufacturer:String,properties_is_login_id:String,properties_v1_element_content:String,properties_v1_message_title:String,properties_v1_message_type:String,properties_v1_symbol:String,properties_v1_stock_name:String,properties_v1_lcs_name:String,properties_v1_lcs_id:String,properties_v1_isfirst:String,properties_v1_share_channel:String,properties_v1_source:String,properties_v1_order:String,properties_v1_custom_params:String,properties_v1_custom_params2:String,properties_v1_environment:String,properties_v1_page_url:String,properties_v1_remain:String,properties_v1_paly_time:String,properties_v1_ispush:String,properties_v1_ishot:String,properties_v1_message_id:String)

}
