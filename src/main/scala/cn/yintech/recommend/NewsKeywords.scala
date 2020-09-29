package cn.yintech.recommend

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.sql.SparkSession

object NewsKeywords {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("TfIdfExample")
      .getOrCreate()

    val path = "E:\\work\\004_idea_workspace\\bigdataProj\\src\\main\\resources\\news\\*"
    val news = spark.sparkContext.wholeTextFiles(path)
    val filename = news.map(_._1)

    import scala.collection.JavaConverters._
    val stopWords = spark.sparkContext.textFile("E:\\work\\004_idea_workspace\\bigdataProj\\src\\main\\resources\\words\\zh-stopWords.txt").collect().toSeq.asJava //构建停词

    val filter = new StopRecognition().insertStopWords(stopWords) //过滤停词
    filter.insertStopNatures("w", null, "null") //根据词性过滤
    val splitWordRdd = news.map(file => { //使用中文分词器将内容分词：[(filename1:w1 w3 w3...),(filename2:w1 w2 w3...)]
      val str = ToAnalysis.parse(file._2).recognition(filter).toStringWithOutNature(" ")
      (file._1, str.split(" "))
    })
    val df = spark.createDataFrame(splitWordRdd).toDF("fileName", "words")

    val hashingTF = new org.apache.spark.ml.feature.HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(200000)
    val documents = hashingTF.transform(df)
    val idf = new org.apache.spark.ml.feature.IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(documents)

    val idfData = idfModel.transform(documents)

    idfData.select("fileName", "features").createTempView("a")
    spark.sql("select fileName,k,v from a lateral view explode(features)  as k,v").show()

    spark.stop()
  }
}
