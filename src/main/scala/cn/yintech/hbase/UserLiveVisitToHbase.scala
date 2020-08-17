package cn.yintech.hbase

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

object UserLiveVisitToHbase {
  /**
   * @param args:T4日（需要跟新数据的日期）'yyyy-mm-dd'格式日期参数，增量更新T4日数据
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UserLiveVisitToHbase")
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
    hconf.set(TableOutputFormat.OUTPUT_TABLE, "user_live_visit_lcs")
    val job = Job.getInstance(hconf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])



    import spark.implicits._
    val value = spark.sql(
  """
    |SELECT DISTINCT get_json_object(properties,'$.userID') userId,
    |       get_json_object(properties,'$.v1_message_id') extraId
    |FROM dwd.dwd_base_event_1d
    |WHERE dt > '2020-02-23'
    |  AND event = 'LiveVisit'
    |  AND get_json_object(properties,'$.v1_element_content')='视频直播播放'
    |  AND get_json_object(properties,'$.userID') != ''
    |  AND get_json_object(properties,'$.userID') is not NULL
    |  AND get_json_object(properties,'$.v1_message_id') != ''
    |  AND get_json_object(properties,'$.v1_message_id') is not null
     |""" .stripMargin)
  .map(row => (row.getString(0),row.getString(1)))
      .rdd
      .groupByKey()
      .map(r => {
        val userId = r._1
        val extraIdStr = r._2.toList.mkString("-")
        val rowKey = userId.reverse

        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("extraIds"), Bytes.toBytes(extraIdStr))
        (new ImmutableBytesWritable, put)
      })


    value.saveAsNewAPIHadoopDataset(job.getConfiguration)

    spark.stop()

  }


}
