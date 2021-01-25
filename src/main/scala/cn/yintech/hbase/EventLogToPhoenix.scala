package cn.yintech.hbase

import cn.yintech.hbase.EventLogToHbase.jsonParse
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.phoenix.spark._
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Statement}
import java.util.UUID

import scala.util.Random

object EventLogToPhoenix {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
            .master("local[*]")
      .appName("EventLogToPhoenix")
      .enableHiveSupport()
      .getOrCreate()


    //    val configuration = new Configuration()
    //    configuration.set("hbase.zookeeper.quorum", "59.110.168.230:2181")
    //    val df = spark.sqlContext.phoenixTableAsDataFrame("base_event_log_test2", Array("rowkey", "userid", "event"), conf = configuration)
    //    df.show()


    import spark.implicits._
    val test = Seq(("1582646387941","$pageview","{\"userID\":\"123\",\"deviceId\":\"aaa\"}")).toDF("time","event","properties")

//    val value = spark.sql(s"select * from dwd.dwd_base_event_1d WHERE dt = '2020-12-16' and event is not null and event != '' and event != 'NativeAppQuotLog' limit 10")
//      .select("time", "event", "properties")

    test
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
        val url = "jdbc:phoenix:59.110.168.230:2181"
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

  def getConnection: Connection = {
    val driver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val url = "jdbc:phoenix:bigdata002:2181"
    var conn: Connection = null
    try Class.forName(driver)
    catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
    }
    try conn = DriverManager.getConnection(url)
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    conn
  }


}
