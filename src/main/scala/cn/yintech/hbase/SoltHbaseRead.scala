package cn.yintech.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

object SoltHbaseRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
        .master("local[*]")
        .appName("SoltHbaseRead")
        .getOrCreate()
    
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "39.107.113.198:2181,59.110.168.230:2181,47.95.237.41:2181")
//    conf.set("hbase.zookeeper.quorum", "bigdata002:2181,bigdata003:2181,bigdata004:2181")
    conf.set(TableInputFormat.INPUT_TABLE, "user_live_visit_lcs")
    conf.set(TableInputFormat.SCAN_ROW_START, "27236788")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "27236789")
    conf.set(TableInputFormat.SCAN_MAXVERSIONS, "20")

    val hbaseRdd = spark.sparkContext.newAPIHadoopRDD(conf, classOf[SaltRangeTableInputFormat],
//    val hbaseRdd = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[ImmutableBytesWritable],
    classOf[Result])



    hbaseRdd.collect().foreach { case (_, result) =>
      val rowKey = Bytes.toString(result.getRow)
      val cell = result.listCells()
      cell.foreach { item =>
        val family = Bytes.toString(item.getFamilyArray, item.getFamilyOffset, item.getFamilyLength)
        val qualifier = Bytes.toString(item.getQualifierArray,
        item.getQualifierOffset, item.getQualifierLength)
        val value = Bytes.toString(item.getValueArray, item.getValueOffset, item.getValueLength)
        println(rowKey + " \t " + "column=" + family + ":" + qualifier + ", " +
        "timestamp=" + item.getTimestamp + ", value=" + value)
        }
      }
    }
}