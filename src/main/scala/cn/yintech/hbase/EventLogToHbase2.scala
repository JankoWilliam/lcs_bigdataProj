package cn.yintech.hbase

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

import scala.util.Random
import scala.util.matching.Regex

object EventLogToHbase2 {
  /**
   * @param args :T4日（需要跟新数据的日期）'yyyy-mm-dd'格式日期参数，增量更新T4日数据
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventLogToHbase")
      .enableHiveSupport()
      .getOrCreate()

    val value = spark.sql(s"select * from dwd.dwd_base_event_1d WHERE dt = '2020-12-16' and event is not null and event != '' and event != 'NativeAppQuotLog' limit 10")
      .select("time", "event", "properties")
      .rdd
      .map(row => {
        //        val sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dataMap = jsonParse(row.getString(2))
        val userId = dataMap.getOrElse("userID", "")
        val deviceId = dataMap.getOrElse("deviceId", "")
        // +---------+---------+-------+--------+-----------+-------------+
        //| userid  | time  | event  | deviceid  | properties  |
        //+---------+---------+-------+--------+-----------+-------------+
        (userId, row(0).toString, row(1).toString, deviceId, row(2).toString)
      })
      //      .filter(_._4 != "null")
      .foreachPartition(rdd => {
        val driver = "org.apache.phoenix.jdbc.PhoenixDriver"
        //        val url = "jdbc:phoenix:59.110.168.230:2181"
        val url = "jdbc:phoenix:bigdata002:2181"
        Class.forName(driver)
        val conn: Connection = DriverManager.getConnection(url)
        rdd.foreach(v => {
          var stmt: PreparedStatement = null
          try {
            val timestamp = v._2.toLong
            //        val rowKey = (Random.nextInt(90) + 10) + "-" + userId + "-" + sdf.parse(sdf.format(time)).getTime
            val rowKey = (Random.nextInt(6) + 'A').toChar + "|" + v._1 + "|" + (Long.MaxValue - timestamp)

            val sql = "upsert into \"base_event_log_test3\" values(?,?,?,?,?,?)"
            stmt = conn.prepareStatement(sql)
            stmt.setString(1, rowKey)
            stmt.setString(2, v._1)
            stmt.setString(3, v._2)
            stmt.setString(4, v._3)
            stmt.setString(5, v._4)
            stmt.setString(6, v._5)
            stmt.executeUpdate(sql)
            conn.commit()
          }
          catch {
            case e: SQLException =>
              e.printStackTrace()
          } finally try {
            if (stmt != null)
              stmt.close()
          } catch {
            case e: SQLException =>
              e.printStackTrace()
          }
        })

        try {
          if (conn != null)
            conn.close()
        } catch {
          case e: SQLException =>
            e.printStackTrace()
        }
      })

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
