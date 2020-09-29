package cn.yintech.longliangqi

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.util.Calendar

import cn.yintech.longliangqi.Caixun_Keywords.stopWordTable
import org.ansj.recognition.impl.StopRecognition
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, IDFModel}
import org.apache.spark.ml.linalg.SparseVector

import scala.collection.mutable.{ArrayBuffer, ListBuffer, WrappedArray}
import org.ansj.util.MyStaticValue
import org.ansj.library.DicLibrary
import org.ansj.splitWord.analysis.NlpAnalysis

import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex

object Caixun_Keywords {

  var stopWordTable: Array[String] = null

  def main(args: Array[String]): Unit = {

    //  获取日期分区参数
    require(!(args == null || args.length != 2 ), "Required 'startDt & endDt' args")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome1 = pattern findFirstIn args(0)
    val dateSome2 = pattern findFirstIn args(1)
    require(dateSome1.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    require(dateSome2.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(1)}")
    val startDt = dateSome1.get // 实际使用yyyy-mm-dd格式日期
    val endDt = dateSome2.get // 实际使用yyyy-mm-dd格式日期
    println("startDate dt : " + startDt + ",endDate dt : "+endDt)

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("article_Portrait")
      .config("spark.debug.maxToStringFields", 100)
      .getOrCreate()

    stopWordTable = spark.sparkContext.textFile("hdfs:///user/licaishi/StopWordTable.txt").collect()

    //隐式转换
    import spark.implicits._

    //直连mysql读取数据，训练集，N天
    //    val lineDataSet_7day = spark.read.format("jdbc").
    //      option("url", "jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false").
    //      option("driver", "com.mysql.jdbc.Driver").
    //      option("user", "licaishi_w").
    //      option("password", "a222541420a50a5").
    //      option("dbtable", "(select id,ind_id,title,freeContent,payContent,summary from lcs_new_caixun_spider where showTime > '2020-08-18')t").
    //      load().cache()

    // 连接hive 拉取数据
    val lineDataSet_7day = spark.sql(s"select id,ind_id,title,freeContent,payContent,summary from ods.ods_lcs_new_caixun_spider_di where dt >= '2020-01-01'")

    //30日数据训练 tf-idf模型
    val idfModel: IDFModel = getTfidfModel_30day(lineDataSet_7day, spark)

    //tf-idf 模型保存
    idfModel.write.overwrite().save("hdfs:///user/licaishi/tfidf_model")


    getBetweenDates(startDt,endDt).foreach( dt => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")

      val cal1 = Calendar.getInstance()
      cal1.setTime(sdf.parse(dt))
      cal1.add(Calendar.DAY_OF_YEAR, 1)
      val dt_add1 = sdf.format(cal1.getTime)

      println(dt + "====================" + dt_add1)

      //直连 mysql 读取数据,当天
      val lineDataSet = spark.read.format("jdbc").
        option("url", "jdbc:mysql://j8h7qwxzyuzs6bby07ek-rw4rm.rwlb.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false").
        option("driver", "com.mysql.jdbc.Driver").
        option("user", "licaishi_w").
        option("password", "a222541420a50a5").
        option("dbtable", s"(select id,ind_id,title,freeContent,payContent,summary from lcs_new_caixun_spider where showTime between '$dt' and '$dt_add1')t").
        load().cache()


      //拿到资讯分词
      val split_words_ansj = getSplitWords_ansj(lineDataSet, spark)

      //得到 CV 词频模型,统计每个分词出现的次数
      val cvModel = getCvModel(split_words_ansj)

      //使用模型，得到结果
      val cvData = cvModel.transform(split_words_ansj)

      //tf-idf 模型调用
      val tf_idfModel = IDFModel.load("hdfs:///user/licaishi/tfidf_model")

      //使用IDF算法进行词频特征修正
      val idfData = tf_idfModel.transform(cvData)

      //取出文章中词频最高的10个单词
      val keyWordsDs = getKeyWordTop(cvModel, idfData, spark)

      //关键词排序
      import spark.implicits._
      val result = keyWordsDs.rdd.map(x => (x, 1)).reduceByKey(_ + _).sortBy(-_._2).toDF("keyword", "count")

      //结果展示
      result.rdd.saveAsTextFile(s"hdfs:///user/licaishi/tfidf_result/$dt")
    })

    spark.stop()

  }

  //分词
  def getSplitWords_ansj(data: DataFrame, spark: SparkSession) = {

    //隐式转换
    import spark.implicits._

    //设置停用词规则，包括停用词表、词性等
    //数字和量词合并
    MyStaticValue.isQuantifierRecognition = true

    val stop = new StopRecognition()
    stop.insertStopNatures("p", "e", "c", "r", "u", "x", "o") //剔除助词、叹词、拟声词等
    stop.insertStopNatures("w") //剔除标点
    stop.insertStopNatures("null")
    stop.insertStopNatures("<br />")
    stop.insertStopRegexes("^一.{0,2}", "^二.{0,2}", "^三.{0,2}", "^四.{0,2}", "^五.{0,2}",
      "^六.{0,2}", "^七.{0,2}", "^八.{0,2}", "^九.{0,2}", "^十.{0,2}")
    stop.insertStopRegexes(".{0,1}") //剔除只有一个汉字的
    stop.insertStopRegexes("^[0-9]+")
    stop.insertStopRegexes("^[a-zA-Z]{1,}") //剔除分词只为英文字母的
    stop.insertStopRegexes("[^a-zA-Z0-9\u4e00-\\u9fa5]+") //把不是汉字、英文、数字的剔除
    stop.insertStopRegexes("^[0-9]+\\%")
    stop.insertStopWords("【","】")
    //添加停用词表
    //    val stopWordTable = Thread.currentThread().getContextClassLoader.getResource("StopWordTable.txt").getPath
    //    val source = Source.fromFile("StopWordTable.txt","GBK")
    for (line <- stopWordTable) {
      stop.insertStopWords(line)
    }

    //定义词性集合
    val list = List("nr", "nt", "ns", "", "userDefine")

    val split_words = data.map(row => {
      //自定分词词典
      DicLibrary.insert(DicLibrary.DEFAULT, "苹果发布会")

      //合并文章标题、正文内容
      val sentence: String = row.getString(2) + " " + row.getString(5) + " " + row.getString(4) + " " + row.getString(5)
      //分词结果的封装,是一个List<Term>
      val result = NlpAnalysis.parse(sentence).recognition(stop)
      //拿到terms
      val terms = result.getTerms
      var ansj_row = ArrayBuffer[String]()
      for (i <- 0 to terms.size() - 1) {
        val word = terms.get(i).getName //拿到词
        val natureStr = terms.get(i).getNatureStr //拿到词性
        //        if((natureStr.startsWith("n") & natureStr != "n") || natureStr.equals("userDefine"))
        if (list.contains(natureStr))
          ansj_row += word
      }
      (row.getInt(0), row.get(1).toString, sentence, ansj_row)
    }).toDF("id", "channel_id", "sentence", "words")
    split_words
  }

  //词频统计
  def getCvModel(split_words: DataFrame) = {
    // CountVectorizer 统计词频，转换成特征向量，并可以得到转换前的单词
    //训练模型
    val cvModel = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").setVocabSize(200 * 10000).setMinDF(0).fit(split_words)
    cvModel
  }

  //统计每篇文章词频最高的10个词
  def getKeyWordTop(cvModel: CountVectorizerModel, idfData: DataFrame, spark: SparkSession) = {

    //隐式转换
    import spark.implicits._
    val voc: Array[String] = cvModel.vocabulary
    // SparseVector 由 indices 和 values 组成。 格式为：(4,[0,2,3],[1.0,1.0,3.0])，其中4为长度，第一个数组为indices，第二个数组为values。
    val resultDf = idfData.map(row => {
      //取出SparseVector中的索引以及得分
      val arrW: Array[Int] = row(5).asInstanceOf[SparseVector].indices
      val arrV: Array[Double] = row(5).asInstanceOf[SparseVector].values
      //降序排序，取出top20个关键词索引
      val sort_indexAndValue = (arrW zip arrV).toList.sortBy(-_._2).take(20).toMap.map(x => (x._1, x._2)).toArray
      var key_words = ArrayBuffer[String]()
      for (elem <- sort_indexAndValue) {
        //将文章关键词按权重大小依次放至集合中
        key_words += voc(elem._1)
      }
      (row.getInt(0), row.getString(1), key_words)
    }).flatMap(_._3)
    //      .toDF("articleId","channel_id","keywords")
    resultDf
  }

  //训练7天数据的 if-idf模型
  def getTfidfModel_30day(lineDataSet_7day: DataFrame, spark: SparkSession) = {

    val split_words_ansj = getSplitWords_ansj(lineDataSet_7day, spark).repartition(10)

    //得到 CV 词频模型
    val cvModel = getCvModel(split_words_ansj)

    //使用模型，得到结果
    val cvData = cvModel.transform(split_words_ansj)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //训练tf-idf模型
    val idfModel = idf.fit(cvData)

    idfModel
  }

  def getBetweenDates(start: String, end: String) = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startData = sdf.parse(start); //定义起始日期
    val endData = sdf.parse(end); //定义结束日期
    var buffer = new ListBuffer[String]

    val tempStart = Calendar.getInstance()
    tempStart.setTime(startData)
    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)

    while (!tempStart.after(tempEnd)) {
      buffer += sdf.format(tempStart.getTime)
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    buffer.toList
  }



}
