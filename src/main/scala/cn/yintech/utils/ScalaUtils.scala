package cn.yintech.utils

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

object ScalaUtils {



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
