package cn.yintech.recommend

import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SparkSession}

object MultilevelTrain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("MultilevelTrain").getOrCreate()

    import spark.implicits._
    val data = spark.sparkContext.textFile("C:\\Users\\zhanpeng.fu\\Desktop\\toutiao-multilevel-text-classfication-dataset\\mlc_dataset_part_aa")
//    val data = spark.sparkContext.textFile("/user/licaishi/mlc_dataset_part_aa")
      .filter(_.split("\\|,\\|",5).size == 5)
      .flatMap {
        x =>
          val array = x.split("\\|,\\|",5)
          array(1).split(",").map((array(0),_,array(2),array(3),array(4),(array(3)+","+array(4)).replace(","," ")))
      }.filter( v => v._6.trim.length > 0 )
      .toDF("id","label_str","title","keyword","news_label","total_words")

    val stringIndexer = new StringIndexer().setInputCol("label_str").setOutputCol("label")
    val dataStringIndexer = stringIndexer.fit(data).transform(data)
    //将词语转换成数组
    val tokenizer = new Tokenizer().setInputCol("total_words").setOutputCol("words")
    val wordsData = tokenizer.transform(dataStringIndexer)

    //70%作为训练数据，30%作为测试数据
    val splits = wordsData.randomSplit(Array(0.1, 0.01))
    val trainingDF = splits(0)
    val testDF = splits(1)


    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(trainingDF)

    //计算每个词的TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    //转换成Bayes的输入格式
    val trainDataRdd = rescaledData.select($"label",$"features").map {
      case Row(label: Double, features: Vector) =>
        LabeledPoint(label, Vectors.dense(features.toArray))
    }
    trainDataRdd.show
    //训练模型
    val model = NaiveBayes.train(trainDataRdd.rdd)

    //测试数据集，做同样的特征表示及格式转换
//    val testfeaturizedData = hashingTF.transform(testDF)
//    val testrescaledData = idfModel.transform(testfeaturizedData)
//    val testDataRdd = testrescaledData.select($"label",$"features").map {
//      case Row(label: Double, features: Vector) =>
//        LabeledPoint(label , Vectors.dense(features.toArray))
//    }

    //对测试数据集使用训练模型进行分类预测
//    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))
//
//    //统计分类准确率
//    val testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
//    println(testaccuracy)

//    val test = spark.sqlContext.createDataFrame(Seq(
//      ("1","经济 转型的 期间 科技类 将成为 比较 明确的 热点 智能穿戴 智能家居 3D打印 电动汽车 等领域 甚至 会产生 改变 世界 的方向")
//    )) toDF("id","text")
//    val dataTokenizer = tokenizer.transform(test)
//
//    dataTokenizer.show()
//    val dataHashTF = hashingTF.transform(dataTokenizer)
//    val dataIdfModel = idfModel.transform(dataHashTF)
//    val dataPredict = dataIdfModel.select($"id",$"features").map {
//      case Row(label: String, features: Vector) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }
//    dataPredict.map( p => model.predict(p.features)).show()

  }
}
