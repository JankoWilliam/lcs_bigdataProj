package cn.yintech.hbase

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.phoenix.spark._
import org.apache.commons.dbcp.ConnectionFactory

import scala.util.Random
import scala.util.matching.Regex

object SparkOnPhoenixEventLogToHbase {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      //      .master("local[*]")
      .appName("SparkOnPhoenixEventLogToHbase")
      .enableHiveSupport()
      .getOrCreate()

    //  获取日期分区参数
    require(!(args == null || args.length == 0 || args(0) == ""), "Required 'dt' arg")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome = pattern findFirstIn args(0)
    require(dateSome.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    val dt = dateSome.get // 实际使用yyyy-mm-dd格式日期
    println("update dt : " + dt)

    import spark.implicits._
    val value = spark.sql(s"select * from dwd.dwd_base_event_1d WHERE dt = '$dt' and event is not null and event != '' and event != 'NativeAppQuotLog' ")
      .select("time", "event", "properties", "distinct_id", "project")
      .rdd
      .map(row => {
        //        val sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dataMap = jsonParse(row.getString(2))
        var userId = dataMap.getOrElse("userID", "")
        var deviceId = dataMap.getOrElse("deviceId", "")
        val distinct_id = row.getString(3)
        val project = row.getString(4)
        // 如果userID为空取deviceId的值，如果deviceId的值也为空就取distinct_id的值，须保证存入phoenix的userid字段不能为空
        if (userId.length == 0) {
          if (deviceId.length == 0) {
            userId = distinct_id
            deviceId = distinct_id
          } else {
            userId = deviceId
          }
        }
        val timestamp = row.getString(0).toLong
        val rowKey = (Random.nextInt(6) + 'A').toChar + "|" + userId + "|" + (Long.MaxValue - timestamp)
        //+---------+---------+---------+-------+--------+-----------+-------------+
        //| rowkey  | userid  | time  | event  | deviceid  | project  | properties  |
        //+---------+---------+---------+-------+--------+-----------+-------------+
        (rowKey, userId, row(0).toString, row(1).toString, deviceId, project, row(2).toString)
      })
      .toDF("\"rowkey\"", "\"cf1.userid\"", "\"cf1.time\"", "\"cf1.event\"", "\"cf1.deviceid\"", "\"cf1.project\"", "\"cf1.properties\"")
      .repartition(33)


    value.saveToPhoenix(Map("table" -> "\"base_event_log_hbase\"", "zkUrl" -> "bigdata002,bigdata003,bigdata004:2181"))


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

}
