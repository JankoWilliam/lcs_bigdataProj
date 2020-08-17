package cn.yintech.eventLog

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser


object JsonParseTestFull {
  def main(args: Array[String]): Unit = {
    var map = Map[String, String]()
    val str = "{\"_track_id\":-2096097433,\"time\":1578690028043,\"type\":\"track\",\"distinct_id\":\"550c3383cfbbdef6\",\"lib\":{\"$lib\":\"Android\",\"$lib_version\":\"3.2.8\",\"$app_version\":\"4.3.13\",\"$lib_method\":\"code\",\"$lib_detail\":\"com.sensorsdata.analytics.android.sdk.SensorsDataAPI##trackEvent##SensorsDataAPI.java##104\"},\"event\":\"$AppClick\",\"properties\":{\"$lib\":\"Android\",\"$os_version\":\"8.1.0\",\"$device_id\":\"550c3383cfbbdef6\",\"$lib_version\":\"3.2.8\",\"$model\":\"PBAM00\",\"$os\":\"Android\",\"$screen_width\":720,\"$screen_height\":1520,\"$manufacturer\":\"OPPO\",\"$app_version\":\"4.3.13\",\"$carrier\":\"中国联通\",\"ABTest\":\"discovery\",\"userID\":\"\",\"deviceId\":\"550c3383cfbbdef6\",\"PushStatus\":false,\"$utm_source\":\"yingyongbao\",\"$utm_campaign\":\"yingyongbao\",\"stock_num\":0,\"planner_num\":\"-2\",\"$wifi\":true,\"$network_type\":\"WIFI\",\"$element_content\":\"资讯\",\"$element_type\":\"TabHost\",\"$is_first_day\":true,\"$ip\":\"112.43.174.126\",\"$is_login_id\":false,\"$city\":\"乌鲁木齐\",\"$province\":\"新疆\",\"$country\":\"中国\"},\"_flush_time\":1578690042911,\"map_id\":\"550c3383cfbbdef6\",\"user_id\":6447479060931925150,\"recv_time\":1578690044208,\"extractor\":{\"f\":\"(dev=863,ino=9699882)\",\"o\":354074,\"n\":\"access_log.2020011105\",\"s\":5089464224,\"c\":5089464224,\"e\":\"data02.yinke.sa\"},\"project_id\":39,\"project\":\"licaishi\",\"ver\":2}"
    val jsonParser = new JSONParser()
    val outJsonObj: JSONObject = jsonParser.parse(str).asInstanceOf[JSONObject]
    val outJsonKey = outJsonObj.keySet()
    val outIter = outJsonKey.iterator

    while (outIter.hasNext) {
      var outKey = outIter.next()
      var outValue = outJsonObj.get(outKey).toString
      if (outKey.startsWith("_")) outKey = "tmp" + outKey
      if (outValue.startsWith("$"))
        outValue = outValue.replaceFirst("$", "")
      map += (outKey -> outValue)
    }

    print(map.keys)
  }
}
