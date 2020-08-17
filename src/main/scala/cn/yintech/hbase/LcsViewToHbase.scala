package cn.yintech.hbase

import java.text.SimpleDateFormat

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

object LcsViewToHbase {
  /**
   * @param args:T4日（需要跟新数据的日期）'yyyy-mm-dd'格式日期参数，增量更新T4日数据
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LcsViewToHbase")
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
    hconf.set(TableOutputFormat.OUTPUT_TABLE, "lcs_view_test")
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
    val value = spark.sql(s"select * from ods.ods_lcs_view_1d  where p_uid is not null and c_time is not null and c_time != ''")
      .select("id","p_uid","ind_id","title","summary","content","c_time","p_time")
      .rdd
      .map(row => {
        val sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val id = if (null != row(0)) row(0).toString else ""
        val p_uid = if (null != row(1)) row(1).toString else ""
        val ind_id = if (null != row(2)) row(2).toString else ""
        val title = if (null != row(3)) row(3).toString else ""
        val summary = if (null != row(4)) row(4).toString else ""
        val content = if (null != row(5)) row(5).toString else ""
        val c_time = if (null != row(6)) row(6).toString else ""
        val p_time = if (null != row(7)) row(7).toString else ""

        val rowKey = (Random.nextInt(6) + 'A').toChar + "-" + p_uid + "-" + sdf.parse(c_time).getTime

        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("title"), Bytes.toBytes(title))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("summary"), Bytes.toBytes(summary))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("content"), Bytes.toBytes(content))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("id"), Bytes.toBytes(id))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ind_id"), Bytes.toBytes(ind_id))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("p_time"), Bytes.toBytes(p_time))
        (new ImmutableBytesWritable, put)
      })


    value.saveAsNewAPIHadoopDataset(job.getConfiguration)

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
}
