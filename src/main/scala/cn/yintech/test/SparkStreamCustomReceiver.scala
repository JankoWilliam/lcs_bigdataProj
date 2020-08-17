package cn.yintech.test

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
class SparkStreamCustomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.DISK_ONLY){
  //启动的时候调用
  override def onStart(): Unit = {
    println("启动了")
    //创建一个socket
    val socket = new Socket(host,port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    //创建一个变量去读取socket的输入流的数据
    var line = reader.readLine()
    while(!isStopped() && line != null){
      //如果接收到了数据，就是用父类中的store方法进行保存
      store(line)
      //继续读取下一行数据
      line = reader.readLine()
    }

  }

  ////终止的时候调用
  override def onStop(): Unit = {
    println("停止了")
  }
}


object SparkStreamCustomReceiver extends App{
  //配置对象
  val conf = new SparkConf().setAppName("").setMaster("local[2]")
  //创建StreamContext
  val ssc = new StreamingContext(conf,Seconds(5))
  //从socket接收数据
  val lineDStream = ssc.receiverStream(new SparkStreamCustomReceiver("192.168.19.123",6789))
  //统计词频
  val res = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

  res.print()
  //启动
  ssc.start()
  ssc.awaitTermination()
}
