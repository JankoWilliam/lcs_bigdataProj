import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Iterator, Set}

import cn.yintech.eventLog.HistoryEventLogEtl.jsonParse
import cn.yintech.flink.SensorsEvent
import cn.yintech.redisUtil.RedisClient
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.flink.api.java.utils.ParameterTool
import shapeless.ops.nat.Max

import scala.collection.mutable
import scala.util.Random
import scala.util.parsing.json.JSON





object Test {
  def main(args: Array[String]): Unit = {

//    val message = "{\"distinct_id\":\"9a44390604116a27\",\"lib\":{\"$lib\":\"js\",\"$lib_method\":\"code\",\"$lib_version\":\"1.15.10\",\"$app_version\":\"4.7.2\"},\"properties\":{\"$timezone_offset\":-480,\"$screen_height\":2400,\"$screen_width\":1080,\"$lib\":\"js\",\"$lib_version\":\"1.15.10\",\"$latest_traffic_source_type\":\"直接流量\",\"$latest_search_keyword\":\"未取到值_直接打开\",\"$latest_referrer\":\"\",\"platForm\":\"h5\",\"v1_element_content\":\"龙虎榜详情页_扫货榜tab\",\"v1_message_title\":\"\",\"v1_message_id\":\"\",\"v1_message_type\":\"\",\"v1_symbol\":\"\",\"v1_stock_name\":\"\",\"v1_lcs_name\":\"\",\"v1_lcs_id\":\"\",\"v1_source\":\"\",\"v1_order\":\"\",\"v1_share_channel\":\"\",\"v1_paying_user\":\"\",\"v1_environment\":\"0\",\"v1_page_url\":\"https://niu.sinalicaishi.com.cn/lcs/wap/dragonAndTiger.html#/dynamicStar?\",\"deviceId\":\"9a44390604116a27\",\"$is_first_day\":true,\"$os_version\":\"10\",\"$device_id\":\"9a44390604116a27\",\"$model\":\"V1962A\",\"$os\":\"Android\",\"$manufacturer\":\"vivo\",\"$app_version\":\"4.7.2\",\"$wifi\":false,\"$network_type\":\"4G\",\"ABTest\":\"discovery\",\"userID\":\"\",\"PushStatus\":false,\"$utm_source\":\"vivo\",\"$utm_campaign\":\"vivo\",\"stock_num\":-1,\"planner_num\":\"-2\",\"$ip\":\"61.158.147.53\",\"$is_login_id\":false,\"$city\":\"郑州\",\"$province\":\"河南\",\"$country\":\"中国\"},\"anonymous_id\":\"173ea92412d42-02e52bcd27f125-385c3972-288000-173ea92412e23\",\"type\":\"track\",\"event\":\"H5Click\",\"_track_id\":389626389,\"_hybrid_h5\":true,\"time\":1597368334602,\"_flush_time\":1597368342291,\"map_id\":\"9a44390604116a27\",\"user_id\":-5774549082903236300,\"recv_time\":1597368337227,\"extractor\":{\"f\":\"(dev=863,ino=1180255)\",\"o\":775503993,\"n\":\"access_log.2020081409\",\"s\":11680212832,\"c\":11680212834,\"e\":\"data03.yinke.sa\"},\"project_id\":39,\"project\":\"licaishi\",\"ver\":2}"
//    import com.alibaba.fastjson.JSON
//    var event: SensorsEvent = JSON.parseObject(message, classOf[SensorsEvent])
//    print(event.toString)

//    val tool: ParameterTool = ParameterTool.fromPropertiesFile( Thread.currentThread().getContextClassLoader.getResource("kafka.properties").getPath)
//    println(tool.get("group.id"))

    val sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

//    println(sdf.format(new Date(1605680275280L)))
////
    println(sdf.parse("2021-01-01 20:54:08.723").getTime)
//    println(new SimpleDateFormat("yyyy-MM-dd").format(0))
//    sdf2.parse("2020-03-1")
//        val sdf2 =  new SimpleDateFormat("yyyy-MM-dd")
//        println(sdf2.format(new Date()))
//      val jsonParser = new JSONParser()
//    val jsonStr =
//      """
//        |{
//        |		"PlatformType": "iOS",
//        |		"$os_version": "13.3",
//        |		"$url_path": "/FE_vue_wap/trend.html",
//        |		"$carrier": "中国联通",
//        |		"$latest_referrer": "",
//        |		"$utm_campaign": "AppStore",
//        |		"$latest_referrer_host": "",
//        |		"$device_id": "1533FD43-DDB9-407D-9874-5394639D0E82",
//        |		"$lib": "js",
//        |		"$manufacturer": "Apple",
//        |		"planner_number": "1",
//        |		"$os": "iOS",
//        |		"$referrer": "",
//        |		"$model": "iPhone11,8",
//        |		"$url": "http://niu.sinalicaishi.com.cn/FE_vue_wap/trend.html#/trend?symbol=sz300750",
//        |		"platForm": "h5",
//        |		"$latest_search_keyword": "未取到值_直接打开",
//        |		"deviceId": "1533FD43-DDB9-407D-9874-5394639D0E82",
//        |		"$lib_version": "1.14.6",
//        |		"$screen_width": 375,
//        |		"$title": "多空趋势",
//        |		"$screen_height": 812,
//        |		"$is_first_time": false,
//        |		"$network_type": "WIFI",
//        |		"userID": "27250486",
//        |		"$wifi": true,
//        |		"$referrer_host": "",
//        |		"$app_version": "4.3.16",
//        |		"stock_number": "21",
//        |		"$latest_traffic_source_type": "直接流量",
//        |		"PushStatus": true,
//        |		"$utm_source": "AppStore",
//        |		"$is_first_day": true,
//        |		"$ip": "60.222.83.182",
//        |		"$is_login_id": false,
//        |		"$city": "运城",
//        |		"$province": "山西",
//        |		"$country": "中国"
//        |	}
//        |""".stripMargin
//    val value = jsonParser.parse(jsonStr).asInstanceOf[JSONObject]
//    value.put("event","BXClick")
//    println(value.toJSONString())


//    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val cal:Calendar=Calendar.getInstance()
//    cal.add(Calendar.DATE,-1)
//    val yesterday=dateFormat.format(cal.getTime())
//    println(yesterday)


//    val jsonParser = new JSONParser()
//    jsonParser.ensuring(true)
//    val outJsonObj = jsonParser.parse("{sdf}")
//    println(outJsonObj.isInstanceOf[JSONObject])

//    val jsonObj = JSON.parse("{xds}")
//    println(jsonObj == null)

//      .asInstanceOf[JSONObject]
//    val outJsonKey = outJsonObj.keySet()
//    val outIter = outJsonKey.iterator
//    println(outIter.hasNext)

//    val pattern = new Regex("/[^,:{}\\[\\]0-9.\\-+Eaeflnr-u \\n\\r\\t]/")
//    val dateSome = pattern findFirstIn "{fsdf:32}"
//    println(dateSome.isDefined)
//    val ite = Iterator("d","g")
//    val set = ite.toSet
//    println(set)

//    val dataMap = jsonParse("{\n\t\"date\": \"2021-01-03\",\n\t\"$os\": \"iOS\",\n\t\"$wifi\": \"0\",\n\t\"$network_type\": \"4G\",\n\t\"userID\": \"27523338\",\n\t\"$province\": \"四川\",\n\t\"$latest_search_keyword\": \"未取到值_直接打开\",\n\t\"v1_element_content\": \"多空趋势详情页访问\",\n\t\"v1_page_url\": \"https:\\/\\/niu.sylstock.com\\/FE_vue_wap\\/trend.html#\\/trend?symbol=sz002459\",\n\t\"v1_paying_user\": \"\",\n\t\"$city\": \"成都\",\n\t\"$model\": \"iPhone7,2\",\n\t\"$screen_width\": \"375\",\n\t\"v1_lcs_id\": \"\",\n\t\"v1_stock_name\": \"晶澳科技\",\n\t\"$app_version\": \"4.12.1\",\n\t\"stock_number\": \"1\",\n\t\"$country\": \"中国\",\n\t\"$utm_campaign\": \"AppStore\",\n\t\"user_id\": \"-342383039955575831\",\n\t\"$lib_version\": \"1.15.10\",\n\t\"$latest_traffic_source_type\": \"直接流量\",\n\t\"v1_message_type\": \"\",\n\t\"v1_lcs_name\": \"\",\n\t\"v1_order\": \"\",\n\t\"$ip\": \"119.4.252.167\",\n\t\"$screen_height\": \"667\",\n\t\"platform\": \"h5\",\n\t\"v1_environment\": \"0\",\n\t\"$device_id\": \"7F08634F-CA65-4A81-918F-27634DC77782\",\n\t\"planner_number\": \"10\",\n\t\"v1_share_channel\": \"\",\n\t\"$app_id\": \"cn.com.sina.licaishi.client\",\n\t\"$latest_referrer\": \"\",\n\t\"$kafka_offset\": \"476394613001\",\n\t\"$carrier\": \"中国联通\",\n\t\"$os_version\": \"12.4.9\",\n\t\"$is_first_day\": \"0\",\n\t\"pushstatus\": \"1\",\n\t\"v1_message_id\": \"\",\n\t\"v1_message_title\": \"\",\n\t\"$utm_source\": \"AppStore\",\n\t\"deviceid\": \"7F08634F-CA65-4A81-918F-27634DC77782\",\n\t\"$lib\": \"js\",\n\t\"v1_symbol\": \"sz002459\",\n\t\"$timezone_offset\": \"-480\",\n\t\"platformtype\": \"iOS\",\n\t\"v1_source\": \"\",\n\t\"$manufacturer\": \"Apple\"\n}")
//    var tmpMap = dataMap
//    tmpMap -= ("_track_id","time","type","distinct_id","lib","event","_flush_time","map_id","userid","recv_time","extractor","project_id","project","ver")
//    tmpMap += ("userID" -> dataMap.getOrElse("userid", ""))
//    val jsonO = new JSONObject
//    for(v <- tmpMap) {
//      jsonO.put(v._1,v._2)
//    }
//    jsonO.put("deviceId",dataMap.getOrElse("deviceid", ""))
//    jsonO.put("$is_first_time",if (dataMap.getOrElse("$is_first_time", "") == "1") "true" else "false")
//    jsonO.put("$is_first_day",if (dataMap.getOrElse("$is_first_day", "") == "1") "true" else "false")
//  println(jsonO.toJSONString)

//    var map = Map[String, String]()
//    map += ("a"->"1")
//    map += ("b"->"2")
//    map += ("c"->"3")
//
//    println(map)
//
//    map -= "a"
//
//    println(map)
//
//    val jsonO = new JSONObject
//    for(v <- map) {
//      jsonO.put(v._1,v._2)
//    }
//
//    println(jsonO.toJSONString)
//
//    println((Random.nextInt(6) + 'A').toChar)

//    val jedis = RedisClient.pool.getResource
//    val str: String = jedis.hget("lcs:td:user:judge:score", "update:time")
//    println(str)
//    jedis.close()
//
//    println(jsonParse(""""""))
//    println("6370342369518158338|,|news_entertainment/other,news_entertainment|,|李易峰跨年写真陪你度过2016再见 2017 我们一起|,|李易峰,度过2016再见2017,一起".split("\\|,\\|",6).size)


//    val jedis = RedisClient.pool.getResource

//    println((Long.MaxValue-1582646387941L+"A").getBytes().length)

  }
  case class UserBean(name : String , age : Int , Time : Long)
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
