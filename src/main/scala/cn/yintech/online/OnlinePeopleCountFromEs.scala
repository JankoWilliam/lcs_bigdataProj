package cn.yintech.online

import java.util
import java.util.concurrent.TimeUnit

import cn.yintech.esUtil.ESConfig
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

import scala.collection.mutable




class OnlinePeopleCountFromEs(host:String, port:Int) extends Receiver[String](StorageLevel.DISK_ONLY){
  //启动的时候调用
  override def onStart(): Unit = {
    println("启动了")
    var esData = esSearch("now-30s")
    while (!isStopped() && esData != null){
      //如果接收到了数据，就是用父类中的store方法进行保存

      store(esData)
      //继续读取下一行数据
      esData = null
      Thread.sleep(10000)
      esData = esSearch("now-30s")
    }
  }

  ////终止的时候调用
  override def onStop(): Unit = {
    println("停止了")
  }

  def esSearch(gte : String): Iterator[String] = {
    val client = ESConfig.client()
    val boolBuilder = QueryBuilders.boolQuery()
    val sourceBuilder = new SearchSourceBuilder()
    //创建一个socket
    val rangeQueryBuilder = QueryBuilders.rangeQuery("logtime") //新建range条件
    rangeQueryBuilder.gte(gte) //开始时间
    boolBuilder.must(rangeQueryBuilder)
    //        rangeQueryBuilder.gte("2020-02-24T18:00:00.000Z") //开始时间
    //        rangeQueryBuilder.lte("2020-02-25T23:59:59.999Z") //结束时间
    //        rangeQueryBuilder.lte("2020-02-25T23:59:59.999Z") //结束时间

    val sortBuilder = SortBuilders.fieldSort("logtime").order(SortOrder.ASC)// 排训规则

    sourceBuilder.query(boolBuilder) //设置查询，可以是任何类型的QueryBuilder。
    sourceBuilder.from(0) //设置确定结果要从哪个索引开始搜索的from选项，默认为0
    sourceBuilder.size(10000) //设置确定搜素命中返回数的size选项，默认为10
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)) //设置一个可选的超时，控制允许搜索的时间。
    sourceBuilder.sort(sortBuilder)

    //sourceBuilder.fetchSource(new String[] {"fields.port","fields.entity_id","fields.message"}, new String[] {}) //第一个是获取字段，第二个是过滤的字段，默认获取全部
    val searchRequest = new SearchRequest("real_time_count") //索引
    searchRequest.types("real_time_count") //类型
    searchRequest.source(sourceBuilder)

    val searchHits: util.List[SearchHit] = ESConfig.scrollSearchAll(client, 10L, searchRequest)

    System.out.println("total hits : " + searchHits.size())
    import scala.collection.JavaConverters._
    searchHits.iterator().asScala.map(_.getSourceAsString)

  }
}


object OnlinePeopleCountFromEs {
  def main(args: Array[String]): Unit = {
    //配置对象
    val conf = new SparkConf().setAppName("OnlinePeople").setMaster("local[2]")
    //创建StreamContext
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("./updateStateByKey")
    //从socket接收数据
    val lineDStream = ssc.receiverStream(new OnlinePeopleCountFromEs("192.168.19.123",6789))
    //逻辑处理

    lineDStream.map(line => {
      val dataMap = jsonParse(line)
      val is_online = dataMap.getOrElse("is_online","0")
      ((dataMap.getOrElse("extra_id",""),dataMap.getOrElse("uid",""),if (is_online == "") "0" else is_online),dataMap.getOrElse("c_time",""))
    })
    .filter(v => v._1._1 != "" && v._1._2 != "") // extra_id和uid非空
    .groupByKey() // 按（extra_id,uid,0）/（extra_id,uid,1）分组
    .map(v => ((v._1._1,v._1._2),(v._1._3 , v._2.max)))
    .groupByKey()
    .map(v => {
        val is_online = v._2.maxBy(_._2)._1
        (v._1._1,(v._1._2,is_online))
      })
    .updateStateByKey[mutable.Set[String]](updateFunc)
    .map(v => (v._1,v._2.size))
    .filter(_._1.length == 5 )
    .print(20)




    //启动
    ssc.start()
    ssc.awaitTermination()
  }

  val updateFunc  = (currValues: Seq[(String,String)], prevValueState: Option[mutable.Set[String]]) => {
    //通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    // 已累加的值
    // 返回累加后的结果。是一个Option[Int]类型
    val onlineUids = prevValueState.getOrElse(mutable.Set())
    currValues.foreach(v => {
      if (v._2 == "0") {
        onlineUids.remove(v._1)
      }else if (v._2 == "1") {
        onlineUids.add(v._1)
      }
    })
    Some(onlineUids)
  }

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try{
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        var outKey = outIter.next()
        var outValue = outJsonObj.get(outKey).toString
        if (outKey.startsWith("_")) outKey = "tmp" + outKey
        outValue = outValue.replace("$", "")
        map += (outKey -> outValue)
      }
    } catch {
      case ex : Exception => {

      }
    }
    map
  }



}
