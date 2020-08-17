package cn.yintech.test

import org.apache.spark.sql.SparkSession

object SparkUnionTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()

      .appName("SparkSQL Union Example")

      .master("local[*]")

      .getOrCreate()

    import spark.implicits._

    spark.read
      .format("jdbc")
      .option("url", "rm-2zebtm824um01072v.mysql.rds.aliyuncs.com/licaishi")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "lcs_spider")
      .option("password", "fK6[lC5*oD6]")
      .option("dbtable", "lcs_circle_notice")
      .load()

    val df1 = List[(String, String, String)](

      ("io.toutiao.bigdatacoder", "大数据", "person1"),

      ("com.eggpain.zhongguodashujuwang1457", "中国大数据", "person2"),

      ("com.cnfsdata.www", "房产大数据", "person3")

    ).toDF("package_name", "app_name", "user")

    val df2 = List[(String, String, String)](

      ("com.jh.APP500958.news", null, "person4"),

      ("com.eggpain.zhongguodashujuwang1457", "中国大数据", "person2")

    ).toDF("package_name", "name", "user")

    //df1与df2合并，不去重。列名不同并不影响合并

    df1.join(df2,Seq("package_name", "package_name"),"full").show()

    //df1与df2合并，使用distinct去重。列名不同并不影响合并

//    df1.union(df2).distinct().show()

    //df1与df3合并，注意df3与df1列数不同

//    val df3 = List[(String, String)](
//
//      ("com.jh.APP500958.news", "person4"),
//
//      ("com.eggpain.zhongguodashujuwang1457", "person2")
//
//    ).toDF("package_name", "user")
//
//    //为df3增加一列，同时注意顺序，因为union合并是按照位置而不是列名
//
//    df1.union(df3.select($"package_name", lit(null).alias("app_name"), $"user")).show()
//
//    //虽然列数相同且类型匹配，但对应列位置不对
//
//    df1.union(df3.withColumn("app_name", lit(null))).show()

  }
}
