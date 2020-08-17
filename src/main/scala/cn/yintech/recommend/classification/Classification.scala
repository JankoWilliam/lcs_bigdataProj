package cn.yintech.recommend.classification

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Classification {
  //读取分词文件和标签文件，两个文件读取后都是RDD形式，元组的形式返回
  def getDocumentsAndLabels(sc: SparkContext, segPath: String, labelListPath: String): (RDD[Seq[String]], Iterator[String]) = {
    (sc.textFile(segPath).map(_.split(",").toSeq), sc.textFile(labelListPath).collect().toSeq.toIterator)
  }

  //训练函数
  def train(sc: SparkContext, trainSegPath: String, trainLabelListPath: String): NaiveBayesModel = {
    //读取训练集的分词和标签
    val (documents, labelList) = getDocumentsAndLabels(sc, trainSegPath, trainLabelListPath)
    //新建HashingTF类
    val hashingTF: HashingTF = new HashingTF()
    //计算TF值
    val tf: RDD[Vector] = hashingTF.transform(documents)

    //缓存，为了计算快，对功能没有影响
    tf.cache()
    //计算IDF值
    val idf: IDFModel = new IDF(minDocFreq = 3).fit(tf)
    //计算TF-IDF值
    val tfIdf: RDD[Vector] = idf.transform(tf)
    //将TFIDF数据，结合标签，转为LabelPoint数据，LabelPoint是训练函数NaiveBayes.train()的输入数据格式
    val training: RDD[LabeledPoint] = tfIdf.map {
      vector: Vector => LabeledPoint(getDoubleOfLabel(labelList.next()), vector)
    }
    //训练函数训练，
    NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
  }

  //测试函数，参数model为训练集训练后的模型
  def test(sc: SparkContext, testSegPath: String, testLabelListPath: String, model: NaiveBayesModel): Double = {

    //读取测试数据集分词和标签数据
    val (documents, labelList) = getDocumentsAndLabels(sc, testSegPath, testLabelListPath)

    //和训练的步骤差不多
    val hashingTF: HashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf: IDFModel = new IDF(minDocFreq = 3).fit(tf)
    val tfIdf: RDD[Vector] = idf.transform(tf)
    val test: RDD[LabeledPoint] = tfIdf.map {
      vector: Vector => LabeledPoint(getDoubleOfLabel(labelList.next()), vector)
    }
    //预测
    val predictionAndLabel: RDD[(Double, Double)] = test.map((p: LabeledPoint) => {
      val predict: Double = model.predict(p.features)
      println(p.label+" : " + predict)
      (predict, p.label)
    })
    //计算准确率
    1.0 * predictionAndLabel.filter((x: (Double, Double)) => x._1 == x._2).count() / test.count()
  }

  //获取标签对应的Double数值，将标签中的数组作为标签对应的数值
  //C11Space -> 11.0
  def getDoubleOfLabel(label: String): Double = {
    label.split("-")(0).tail.toDouble
  }

  def main(args: Array[String]): Unit = {
    //新建spark上下文
    val conf: SparkConf = new SparkConf().setAppName("Classification").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    //调用处理函数
    println(test(sc, "E:\\work\\BaiduNetdiskDownload\\classification\\test_seg.txt",
      "E:\\work\\BaiduNetdiskDownload\\classification\\test_label.txt", train(sc,
        "E:\\work\\BaiduNetdiskDownload\\classification\\utf-answer_seg.txt",
        "E:\\work\\BaiduNetdiskDownload\\classification\\utf-answer_label_list.txt"
      )
    )
    )
  }
}
