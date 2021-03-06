package cn.yintech.hbase


import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Random
import scala.util.matching.Regex

object EventLogToHbase {
  /**
   * @param args :T4日（需要跟新数据的日期）'yyyy-mm-dd'格式日期参数，增量更新T4日数据
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventLogToHbase")
      .enableHiveSupport()
      .getOrCreate()

    //  获取日期分区参数
    require(!(args == null || args.length == 0 || args(0) == ""), "Required 'dt' arg")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome = pattern findFirstIn args(0)
    require(dateSome.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    val dt = dateSome.get // 实际使用yyyy-mm-dd格式日期
    println("update dt : " + dt)
    /**
     * 初始化hbase
     */
    val hconf = new Configuration()
    hconf.set("hbase.zookeeper.quorum", "bigdata002,bigdata003,bigdata004")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.set(TableOutputFormat.OUTPUT_TABLE, "base_event_log_test")
    val job = Job.getInstance(hconf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])



    // 在存入hbase之前先清空hbase表
    //    val connection = ConnectionFactory.createConnection(hconf)
    //    val admin=connection.getAdmin
    //    val table = TableName.valueOf("base_event_log_test")
    //    if(admin.tableExists(table)){
    //      admin.disableTableAsync(table)
    //      admin.truncateTable(table,false)
    //    }
    //    admin.enableTableAsync(table)

    import spark.implicits._
    val value = spark.sql(s"select * from dwd.dwd_base_event_1d WHERE dt = '$dt' and event is not null and event != '' and event != 'NativeAppQuotLog' ")
      .select("time", "event", "properties")
      .rdd
      .map(row => {
        //        val sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dataMap = jsonParse(row.getString(2))
        val userId = dataMap.getOrElse("userID", "")
        val deviceId = dataMap.getOrElse("deviceId", "")

        (row(0).toString, row(1).toString,row(2).toString, userId,deviceId)
      })
//      .filter(_._4 != "null")
      .map(v => {
        val timestamp = v._1.toLong
        //        val rowKey = (Random.nextInt(90) + 10) + "-" + userId + "-" + sdf.parse(sdf.format(time)).getTime
        val rowKey = (Random.nextInt(6) + 'A').toChar + "|" + v._4 + "|" + (Long.MaxValue-timestamp)
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("time"), Bytes.toBytes(if (null != v._1) v._1 else ""))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("event"), Bytes.toBytes(if (null != v._2) v._2 else ""))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("properties"), Bytes.toBytes(if (null != v._3) v._3 else ""))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("userid"), Bytes.toBytes(if (null != v._4) v._4 else ""))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("deviceid"), Bytes.toBytes(if (null != v._5) v._5 else ""))
        (new ImmutableBytesWritable, put)
      })


    value.saveAsNewAPIHadoopDataset(job.getConfiguration)

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
