package cn.yintech.recommend.classification

import java.io.File
import java.util

import org.ansj.domain.Result
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.commons.io.FileUtils

//scala集合与java集合转换的包，按住Ctrl点进源码，可以查看转换规则
import scala.collection.JavaConversions._

object WordSplit {

  //分词函数
  def corpusSegment(utfCorpusPath: String, utfSegmentPath: String, trainLabelListPath: String, trainSegmentPath: String): Unit = {
    //计数用，统计样本个数
    var count = 0
    //存放标签的Java数组，这里使用java数组是为了方便写入文件
    val labelList = new util.ArrayList[String]()
    //存放分词后字符串的数组，同样为了方便写入文件
    val contextList = new util.ArrayList[String]()
    //打开根目录
    val corpusDir: Array[File] = new File(utfCorpusPath).listFiles()
    //类别目录
    for (corpusClassDir: File <- corpusDir) {
      //每一个文件
      for (utfText <- corpusClassDir.listFiles()) {

        count = count + 1
        //调用分词方法
        val textSeg: Result = ToAnalysis.parse(FileUtils.readFileToString(utfText)
          .replace("\r\n", "") //去除换行和回车
          .replace("\r", "") //去除单独的回车
          .replace("\n", "") //去除单独的换行
          .replace(" ", "") //去除空格
          .replace("\u3000", "") //去除全角空格（中文空格）
          .replace("\t", "") //去除制表符
          .replaceAll(s"\\pP|\\pS|\\pC|\\pN|\\pZ", "") //通过设置Unicode类别的相关正则去除符号
          .trim
        )
        //读取停用词，就是一些对分类没有作用的词，去除可以对特征向量降维
        val stopWordList: Seq[String] = FileUtils.readFileToString(new File("E:\\work\\BaiduNetdiskDownload\\classification\\stop_word_chinese.txt"))
          .split("\r\n").toSeq
        //新建停用词对象
        val filter = new StopRecognition()
        //加载停用词列表
        filter.insertStopWords(stopWordList)
        //去除停用词
        val result: Result = textSeg.recognition(filter)

        /**
         *这里如果将每篇文章的分词单独写入一个文件，则在构建词向量时，spark
         * 就要分别读取每篇文章的分词，而spark每读一个文件，就会就会产生一个RDD，
         * 这样读取所有文本的分词就会产生巨量的RDD，这时把这些分词合并到一个集合中（巨量的RDD
         * 合并成一个RDD）时，spark在构建DAG时就会爆掉（亲身经历，当时用的时RDD的union方法）
         */

        //将分词内容加入列表
        contextList.add(result.toStringWithOutNature)
        //将标签加入列表，标签的顺序和文本分词后的顺序是对应的
        labelList.add(corpusClassDir.getName)

      }
    }
    println(count)
    //将分词写入文件
    FileUtils.writeLines(new File(trainSegmentPath), "UTF-8", contextList)
    //将文本标签写入文件
    FileUtils.writeLines(new File(trainLabelListPath), "UTF-8", labelList)

  }

  def splitWord (line : String) : String = {
    val textSeg: Result = ToAnalysis.parse("FileUtils.readFileToString(utfText)"
      .replace("\r\n", "") //去除换行和回车
      .replace("\r", "") //去除单独的回车
      .replace("\n", "") //去除单独的换行
      .replace(" ", "") //去除空格
      .replace("\u3000", "") //去除全角空格（中文空格）
      .replace("\t", "") //去除制表符
      .replaceAll(s"\\pP|\\pS|\\pC|\\pN|\\pZ", "") //通过设置Unicode类别的相关正则去除符号
      .trim
    )
    //读取停用词，就是一些对分类没有作用的词，去除可以对特征向量降维
    val stopWordList: Seq[String] = FileUtils.readFileToString(new File("E:\\work\\BaiduNetdiskDownload\\classification\\stop_word_chinese.txt"))
      .split("\r\n").toSeq
    //新建停用词对象
    val filter = new StopRecognition()
    //加载停用词列表
    filter.insertStopWords(stopWordList)
    //去除停用词
    val result: Result = textSeg.recognition(filter)
    ""
  }

  def main(args: Array[String]): Unit = {
    //这里该了一些目录结构，对代码的功能没有影响
    corpusSegment("E:\\work\\BaiduNetdiskDownload\\classification\\utf-answer", "E:\\work\\BaiduNetdiskDownload\\classification\\utf-answer_segment\\",
      "E:\\work\\BaiduNetdiskDownload\\classification\\utf-answer_label_list.txt", "E:\\work\\BaiduNetdiskDownload\\classification\\utf-answer_seg.txt")

//    corpusSegment("./test/utf_test_corpus/", "./test/utf_test_segment/",
//      "./test/test_label_list.txt", "./test/test_seg.txt")
  }
}
