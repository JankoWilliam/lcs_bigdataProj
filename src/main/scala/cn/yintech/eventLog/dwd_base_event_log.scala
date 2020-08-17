package cn.yintech.eventLog

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object dwd_base_event_log {

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
      .appName("dwd_base_event_log")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val data = spark.sql(s"select line from ods.ods_event_log where dt = '$dt' ")
    val value = data.map(row => {
      val line = row.getString(0)
      val dataMap = jsonParse(line)
      val eventLog = EventLogBean(
        dataMap.getOrElse("distinct_id", ""),
        dataMap.getOrElse("tmp_flush_time", ""),
        dataMap.getOrElse("tmp_track_id", ""),
        dataMap.getOrElse("event", ""),
        dataMap.getOrElse("project", ""),
        dataMap.getOrElse("user_id", ""),
        dataMap.getOrElse("map_id", ""),
        dataMap.getOrElse("lib", ""),
        dataMap.getOrElse("properties", ""),
        dataMap.getOrElse("project_id", ""),
        dataMap.getOrElse("ver", ""),
        dataMap.getOrElse("extractor", ""),
        dataMap.getOrElse("time", ""),
        dataMap.getOrElse("type", ""),
        dataMap.getOrElse("recv_time", "")
      )
      eventLog
    })
    //      value.show(10)

    value.createOrReplaceGlobalTempView("eventlog")

    spark.sql(s"insert into dwd.dwd_base_event_log partition(dt='$dt')" +
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
    try {
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        val outKey = outIter.next()
        val outValue = outJsonObj.get(outKey).toString
//        if (outKey.startsWith("_")) outKey = "tmp" + outKey
//        outValue = outValue.replace("$", "")
        map += (outKey -> outValue)
      }
    } catch {
      case ex : Exception => {

      }
    }
    map
  }

  case class EventLogBean(
                           distinct_id : String,
                           tmp_flush_time: String,
                           tmp_track_id: String,
                           event: String,
                           project: String,
                           user_id: String,
                           map_id: String,
                           lib: String,
                           properties: String,
                           project_id: String,
                           ver: String,
                           extractor: String,
                           time: String,
                           `type`: String,
                           recv_time: String
                         )
}