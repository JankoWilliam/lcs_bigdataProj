import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Iterator, Set}

import cn.yintech.flink.SensorsEvent
import cn.yintech.redisUtil.RedisClient
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.mutable
import scala.util.Random
import scala.util.parsing.json.JSON





object Test {
  def main(args: Array[String]): Unit = {

    val message = "{\"distinct_id\":\"9a44390604116a27\",\"lib\":{\"$lib\":\"js\",\"$lib_method\":\"code\",\"$lib_version\":\"1.15.10\",\"$app_version\":\"4.7.2\"},\"properties\":{\"$timezone_offset\":-480,\"$screen_height\":2400,\"$screen_width\":1080,\"$lib\":\"js\",\"$lib_version\":\"1.15.10\",\"$latest_traffic_source_type\":\"直接流量\",\"$latest_search_keyword\":\"未取到值_直接打开\",\"$latest_referrer\":\"\",\"platForm\":\"h5\",\"v1_element_content\":\"龙虎榜详情页_扫货榜tab\",\"v1_message_title\":\"\",\"v1_message_id\":\"\",\"v1_message_type\":\"\",\"v1_symbol\":\"\",\"v1_stock_name\":\"\",\"v1_lcs_name\":\"\",\"v1_lcs_id\":\"\",\"v1_source\":\"\",\"v1_order\":\"\",\"v1_share_channel\":\"\",\"v1_paying_user\":\"\",\"v1_environment\":\"0\",\"v1_page_url\":\"https://niu.sinalicaishi.com.cn/lcs/wap/dragonAndTiger.html#/dynamicStar?\",\"deviceId\":\"9a44390604116a27\",\"$is_first_day\":true,\"$os_version\":\"10\",\"$device_id\":\"9a44390604116a27\",\"$model\":\"V1962A\",\"$os\":\"Android\",\"$manufacturer\":\"vivo\",\"$app_version\":\"4.7.2\",\"$wifi\":false,\"$network_type\":\"4G\",\"ABTest\":\"discovery\",\"userID\":\"\",\"PushStatus\":false,\"$utm_source\":\"vivo\",\"$utm_campaign\":\"vivo\",\"stock_num\":-1,\"planner_num\":\"-2\",\"$ip\":\"61.158.147.53\",\"$is_login_id\":false,\"$city\":\"郑州\",\"$province\":\"河南\",\"$country\":\"中国\"},\"anonymous_id\":\"173ea92412d42-02e52bcd27f125-385c3972-288000-173ea92412e23\",\"type\":\"track\",\"event\":\"H5Click\",\"_track_id\":389626389,\"_hybrid_h5\":true,\"time\":1597368334602,\"_flush_time\":1597368342291,\"map_id\":\"9a44390604116a27\",\"user_id\":-5774549082903236300,\"recv_time\":1597368337227,\"extractor\":{\"f\":\"(dev=863,ino=1180255)\",\"o\":775503993,\"n\":\"access_log.2020081409\",\"s\":11680212832,\"c\":11680212834,\"e\":\"data03.yinke.sa\"},\"project_id\":39,\"project\":\"licaishi\",\"ver\":2}"
    import com.alibaba.fastjson.JSON
    var event: SensorsEvent = JSON.parseObject(message, classOf[SensorsEvent])
    print(event.toString)

//    val tool: ParameterTool = ParameterTool.fromPropertiesFile( Thread.currentThread().getContextClassLoader.getResource("kafka.properties").getPath)
//    println(tool.get("group.id"))

//    val sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
////
//    println(sdf.parse("2020-04-15 09:28:57.942").getTime)
//    println(new SimpleDateFormat("yyyy-MM-dd").format(0))
//    val sdf2 =  new SimpleDateFormat("yyyy-MM-dd")
//    sdf2.parse("2020-03-1")
//    println(sdf2.format(new Date()))

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

//    val jsonO = new JSONObject
//    jsonO.put("name","32")
//    jsonO.put("age",new Integer(10))
//    jsonO.put("time", "32")
//    println(jsonO.toJSONString)


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
