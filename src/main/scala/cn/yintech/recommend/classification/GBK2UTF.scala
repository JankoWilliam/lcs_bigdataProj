package cn.yintech.recommend.classification

import java.io.File

import org.apache.commons.io.FileUtils

object GBK2UTF {

  def GBK2UTF8(GBKCorpusPath: String, UTF8CorpusPath: String): Unit = {
    //打开根目录
    val GBKCorpusDir: Array[File] = new File(GBKCorpusPath).listFiles()
    //对应的UTF-8格式的目录是否存在，不存在新建
    val UTFCorpusDir: File = new File(UTF8CorpusPath);
    if (!UTFCorpusDir.exists()) {
      UTFCorpusDir.mkdir()
    }

    //打开类别目录
    for (gbkClassDir: File <- GBKCorpusDir) {
      //记录目录路径，为创建UTF-8格式的文件夹和文件提供路径
      val UTFClassDirPath: String = UTF8CorpusPath + gbkClassDir.getName
      //UTF-8格式的类别目录是否存在，不存在新建
      val UTFClassDir: File = new File(UTFClassDirPath)
      if (!UTFClassDir.exists()) {
        UTFClassDir.mkdir()
      }

      for (gbkText: File <- gbkClassDir.listFiles()) {
        //将文件以GBK格式读取为字符串，转为UTF-8格式后写入新文件
        FileUtils.write(new File(UTFClassDirPath + "/" + gbkText.getName),
          FileUtils.readFileToString(gbkText, "GBK"), "UTF-8")
      }
    }

  }


  def main(args: Array[String]): Unit = {
    GBK2UTF8("E:\\work\\BaiduNetdiskDownload\\classification\\answer\\",
      "E:\\work\\BaiduNetdiskDownload\\classification\\utf-answer\\")
  }

}
