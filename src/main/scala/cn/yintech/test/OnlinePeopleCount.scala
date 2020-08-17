package cn.yintech.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OnlinePeopleCount {

  val CHECK_POINT_PATH = "hdfs:///user/zhanpeng.fu/mycheck"

  def main(args: Array[String]): Unit = {

    def createFunc():StreamingContext={
      val conf = new SparkConf().setMaster("local[*]").setAppName("test")
      val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
      //日志级别
      ssc.sparkContext.setLogLevel("WARN")
      ssc.checkpoint(CHECK_POINT_PATH)

      //通过监听一个端口得到一个DStream流    数据的输入
      val DStream: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1",6789)
      //数据的处理
      val wordOneDstream = DStream.flatMap(_.split(","))
        .map((_, 1))
      val wordCountDStream = wordOneDstream.updateStateByKey[Int](addFunc)
      //数据的输出
      wordCountDStream.print()

      ssc
    }

    val ssc = StreamingContext.getOrCreate( CHECK_POINT_PATH, createFunc)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
  val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
    //通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    val currentCount = currValues.sum
    // 已累加的值
    val previousCount = prevValueState.getOrElse(0)
    // 返回累加后的结果。是一个Option[Int]类型
    Some(currentCount + previousCount)
  }


}
