package cn.yintech.recommend

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
 
 
object TestNaiveBayes {
  
  case class RawDataRecord(category: String, text: String)
  
  def main(args : Array[String]) {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("TestNaiveBayes")
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    var srcRDD = sc.textFile("C:\\Users\\zhanpeng.fu\\Desktop\\sougou-train").map {
      x => 
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }
    
    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()


    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1：")
//    wordsData.select($"category",$"text",$"words").show()
//
    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")
//    featurizedData.select($"category", $"words", $"rawFeatures").show()
//
//
    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3：")

//    rescaledData.select($"category", $"features").show()
//
    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    println("output4：")
    trainDataRdd.show()
//
    //训练模型
    val model = NaiveBayes.train(trainDataRdd.rdd, lambda = 1.0, modelType = "multinomial")

    //测试数据集，做同样的特征表示及格式转换
//    var testwordsData = tokenizer.transform(testDF)
//    var testfeaturizedData = hashingTF.transform(testwordsData)
//    var testrescaledData = idfModel.transform(testfeaturizedData)
//    var testDataRdd = testrescaledData.select($"category",$"features").map {
//      case Row(label: String, features: Vector) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }

    //对测试数据集使用训练模型进行分类预测
//    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

//    val data = sqlContext.createDataFrame(Seq(
//      ("1","经济 转型的 期间 科技类 将成为 比较 明确的 热点 智能穿戴 智能家居 3D打印 电动汽车 等领域 甚至 会产生 改变 世界 的方向")
//    )) toDF("id","text")
//    val dataTokenizer = tokenizer.transform(data)
//
//    dataTokenizer.show()
//    val dataHashTF = hashingTF.transform(dataTokenizer)
//    val dataIdfModel = idfModel.transform(dataHashTF)
//    val dataPredict = dataIdfModel.select($"id",$"features").map {
//      case Row(label: String, features: Vector) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }
//    dataPredict.map( p => model.predict(p.features)).show()

    //统计分类准确率
//    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
//    println("output5：")
//    println(testaccuracy)
    
  }
}