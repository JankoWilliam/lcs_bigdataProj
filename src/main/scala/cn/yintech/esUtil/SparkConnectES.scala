package cn.yintech.esUtil

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object SparkConnectES {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DecisionTree1").setMaster("local[*]")
    sparkConf.set("es.nodes", "115.28.252.78")
    sparkConf.set("es.port", "9100")
    val sc = new SparkContext(sparkConf)
    val value = sc.esRDD("real_time_count/real_time_count",
      """
        |{
        |  "query": {
        |    "match": {
        |      "uid": "27243518"
        |    }
        |  }
        |}
        |""".stripMargin)


    value.foreach(println)


    sc.stop()
  }


}
