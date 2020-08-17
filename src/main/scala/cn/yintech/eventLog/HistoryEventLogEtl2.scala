package cn.yintech.eventLog

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object HistoryEventLogEtl2 {
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
      .appName("HistoryEventLogEtl")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val data = spark.sql(s"select line from `ods`.`ods_event_log_history_compress` where dt = '$dt' ")
    val value = data
      .map(func = row => {
        val line = if (row(0) == null) "" else row.getString(0)
        val dataMap = jsonParse(line)
        var tmpMap = dataMap
        tmpMap -= ("_track_id","time","type","distinct_id","lib","event","_flush_time","map_id","userid","recv_time","extractor","project_id","project","ver")
        tmpMap += ("userID" -> dataMap.getOrElse("userid", ""))
        val jsonO = new JSONObject
        for(v <- tmpMap) {
          jsonO.put(v._1,v._2)
        }
        val eventLog = EventLogBean(
          dataMap.getOrElse("_track_id", ""),
          dataMap.getOrElse("time", ""),
          dataMap.getOrElse("type", ""),
          dataMap.getOrElse("distinct_id", ""),
          dataMap.getOrElse("lib", ""),
          dataMap.getOrElse("event", ""),
          dataMap.getOrElse("properties", jsonO.toJSONString),
          dataMap.getOrElse("_flush_time", ""),
          dataMap.getOrElse("map_id", ""),
          dataMap.getOrElse("userid", ""),
          dataMap.getOrElse("recv_time", ""),
          dataMap.getOrElse("extractor", ""),
          dataMap.getOrElse("project_id", ""),
          dataMap.getOrElse("project ", ""),
          dataMap.getOrElse("ver", "")
        )
        eventLog
      })
    //      value.show(10)

    value.repartition(10).persist().createOrReplaceGlobalTempView("eventlog")
//          .write
//          .mode(SaveMode.Overwrite)
//          .format("Hive")
//          .saveAsTable("dwd.dwd_base_event_1d_tmp")
    spark.sql(
      s" insert overwrite table dwd.dwd_base_event_api_1d partition(dt='$dt')" +
        " select * from global_temp.eventlog ")


    spark.stop()

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
  case class EventLogBean(
                           _track_id : String,
                           `time`: String,
                           `type`: String,
                           distinct_id: String,
                           lib: String,
                           event: String,
                           properties: String,
                           _flush_time : String,
                           map_id: String,
                           user_id : String,
                           recv_time: String,
                           extractor : String,
                           project_id : String,
                           project : String,
                           ver: String
                         )
}
