package cn.yintech.eventLog

import net.minidev.json.{JSONArray, JSONObject}
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.{SaveMode, SparkSession}

/***
 * (GN500241,
 *  { "plate_code":"GN500241",
 *    "plate_name":"杭州湾大湾区",
 *    "plate_theme":"环杭州湾的区位优势较为突出，处于沿海开放带、长江经济带、长江三角洲城市群与“一带一路”等多重国家战略的交汇点",
 *    "plate_rate":"0.019702500000000",
 *    "symbol":[
 *      {"symbol":"sh600018","name":"上港集团"},
 *      {"symbol":"sh600126","name":"杭钢股份"},
 *      {"symbol":"sh600477","name":"杭萧钢构"},
 *      {"symbol":"sh600496","name":"精工钢构"},
 *      {"symbol":"sh600515","name":"海航基础"},
 *      {"symbol":"sh600798","name":"宁波海运"},
 *      {"symbol":"sh601018","name":"宁波港"},
 *      {"symbol":"sh601789","name":"宁波建工"},
 *      {"symbol":"sz002135","name":"东南网架"},
 *      {"symbol":"sz002244","name":"滨江集团"}]
 *  })
 */
object SparkConnectRedis {
  def main(args: Array[String]): Unit = {


    val spark2 = SparkSession.builder()
    .appName("SparkReadRedis")
//    .master("local[1]")
    .config("spark.redis.host","47.104.254.17")
    .config("spark.redis.port", "9701")
    .config("spark.redis.auth","fG2@bE1^hE4[") //指定redis密码
//    .config("spark.redis.host","192.168.19.123")
//    .config("spark.redis.port", "9700")
//    .config("spark.redis.auth","cW%Kiiuz3Q2aLylk7y%M") //指定redis密码
//          .config("spark.redis.db","0") //指定redis库
    .enableHiveSupport()
    .getOrCreate()

    import com.redislabs.provider.redis._
//    val value  = spark2.sparkContext.fromRedisHash("lcs_plate:symbol:cx99")
    val value  = spark2.sparkContext.fromRedisHash("lcs_ali_plate:symbol:cx99")

    import spark2.implicits._
    val result1 = value.flatMap(line => {
      val symbolJson = jsonParse(line._2).getOrElse("symbol", "")
      val plate_name = jsonParse(line._2).getOrElse("plate_name", "")
      if (symbolJson != "") {
        val jsonParser = new JSONParser()
        val symbols: Array[AnyRef] = jsonParser.parse(symbolJson).asInstanceOf[JSONArray].toArray
        val result = symbols.map(v => {
          val symbolObj = jsonParser.parse(v.toString).asInstanceOf[JSONObject]
          val symbol = symbolObj.get("symbol").toString
          //          val name = symbolObj.get("name").toString
          (line._1 , plate_name , symbol)
        })
        result
      } else {
         Array[(String,String, String)]()
      }
    }).toDF("plate","plate_name","symbol")
          .write
          .mode(SaveMode.Overwrite)
          .format("Hive")
          .saveAsTable("lcs_test.dwd_plate_symbol")


//    result1.write
//          .mode(SaveMode.Overwrite)
//          .format("Hive")
//          .saveAsTable("lcs_test.dwd_plate_symbol")

    // 从redis中读取数据————方法一
//    value.foreach(println)


    spark2.stop()
  }

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try {
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        var outKey = outIter.next()
        var outValue = outJsonObj.get(outKey).toString
        map += (outKey -> outValue)
      }
    }
    catch {
      case ex : Exception => {
        println(ex)
      }
    }
    map
  }

}
