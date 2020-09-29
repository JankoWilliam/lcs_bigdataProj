package cn.yintech.longliangqi

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.util.matching.Regex
import com.redislabs.provider.redis._
import org.apache.spark.broadcast.Broadcast



object td_UserPointLabel {
  case class TdlcsList(lcsID:String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("td_UserPointLabel")
      //      .config("spark.default.parallelism","3")
      //      .master("local[*]")
      .config("spark.redis.host","47.104.254.17")
      .config("spark.redis.port", "9701")
      .config("spark.redis.auth","fG2@bE1^hE4[") //指定redis密码
      //          .config("spark.redis.host","192.168.19.123")
      //          .config("spark.redis.port", "9700")
      //          .config("spark.redis.auth","cW%Kiiuz3Q2aLylk7y%M") //指定redis密码
      //          .config("spark.redis.db","0") //指定redis库
      .enableHiveSupport()
      .getOrCreate()

    //  获取日期分区参数
    require(!(args == null || args.length == 0 || args(0) == ""), "Required 'dt' arg")
    val pattern = new Regex("\\d{4}[-]\\d{2}[-]\\d{2}")
    val dateSome = pattern findFirstIn args(0)
    require(dateSome.isDefined, s"Required PARTITION args like 'yyyy-mm-dd' but find ${args(0)}")
    val dt = dateSome.get // 实际使用yyyy-mm-dd格式日期
    println("update dt : " + dt)

    //    val dt= "2020-04-17"

    //加入隐式转换,RDD转DF、DS时起作用
    import spark.implicits._

    //读取redisTD理财师列表
    val userFollows = spark.sparkContext.fromRedisHash("lcs:td:planner:list")
    //读取redis数据，将其转换set集合
    val lcsIDset: Set[String] = userFollows.map(row => {
      row._1
    }).collect().toSet

    //广播redis中读取到的TD理财师列表
    val lcsIdBC: Broadcast[Set[String]] = spark.sparkContext.broadcast(lcsIDset)

    //读取数据
    val dataFrame: DataFrame = spark.sql(s"select event,properties from dwd.dwd_base_event_1d where dt = '$dt' ")

    //对DeviceID数据过滤  $device_id
    val deviceDF1: Dataset[(String, Int, Int, Int, Int, Int, Int)] = dataFrame.
      map(row => {
        //若event不为null时，才做逻辑处理。防止event事件为null导致的空指针现象发生。
        if (row.getString(1) != null) {
          val properJson = JSON.parseObject(row.getString(1))
          val deviceId = if (properJson.getOrDefault("$device_id", "") != null)
            properJson.getOrDefault("$device_id", "") else ""
          val v1_element_content = if (properJson.getOrDefault("v1_element_content", "") != null)
            properJson.getOrDefault("v1_element_content", "") else ""
          val v1_lcs_id = if (properJson.getOrDefault("v1_lcs_id", "") != null)
            properJson.getOrDefault("v1_lcs_id", "") else ""
          (deviceId.toString, if (row.getString(0) != null) row.getString(0) else "", v1_element_content.toString,v1_lcs_id.toString)
        } else ("", "", "","")
      }).map(row => {
      val lcsIDList: Set[String] = lcsIdBC.value
      if (row._2 == "NativeAppVisit" && row._3 == "TD合约详情页")
        (row._1, 1, 0, 0, 0, 0, 0)
      else if (row._2 == "NativeAppClick" && row._3 == "行情_T+D")
        (row._1, 0, 1, 0, 0, 0, 0)
      else if (row._2 == "H5Visit" && row._3 == "TD开户页")
        (row._1, 0, 0, 1, 0, 0, 0)
      else if (row._2 == "NativeAppVisit" && row._3 == "理财师主页访问" && lcsIDList.contains(row._4))
        (row._1, 0, 0, 0, 1, 0, 0)
      else if (row._2 == "LiveVisit" && row._3 == "视频直播播放" && lcsIDList.contains(row._4))
        (row._1, 0, 0, 0, 0, 1, 0)
      else if (row._2 == "NativeAppVisit" && row._3 == "新闻详情页访问")
        (row._1, 0, 0, 0, 0, 0, 1)
      else
        (row._1, 0, 0, 0, 0, 0, 0)
    })

    //对DeviceID数据过滤  deviceId
    val deviceDF2: Dataset[(String, Int, Int, Int, Int, Int, Int)] = dataFrame.
      map(row => {
        //若event不为null时，才做逻辑处理。防止event事件为null导致的空指针现象发生。
        if (row.getString(1) != null) {
          val properJson = JSON.parseObject(row.getString(1))
          val deviceId = if (properJson.getOrDefault("deviceId", "") != null)
            properJson.getOrDefault("deviceId", "") else ""
          val v1_element_content = if (properJson.getOrDefault("v1_element_content", "") != null)
            properJson.getOrDefault("v1_element_content", "") else ""
          val v1_lcs_id = if (properJson.getOrDefault("v1_lcs_id", "") != null)
            properJson.getOrDefault("v1_lcs_id", "") else ""
          (deviceId.toString, if (row.getString(0) != null) row.getString(0) else "", v1_element_content.toString,v1_lcs_id.toString)
        } else ("", "", "","")
      }).map(row => {
      val lcsIDList: Set[String] = lcsIdBC.value
      if (row._2 == "NativeAppVisit" && row._3 == "TD合约详情页")
        (row._1, 1, 0, 0, 0, 0, 0)
      else if (row._2 == "NativeAppClick" && row._3 == "行情_T+D")
        (row._1, 0, 1, 0, 0, 0, 0)
      else if (row._2 == "H5Visit" && row._3 == "TD开户页")
        (row._1, 0, 0, 1, 0, 0, 0)
      else if (row._2 == "NativeAppVisit" && row._3 == "理财师主页访问" && lcsIDList.contains(row._4))
        (row._1, 0, 0, 0, 1, 0, 0)
      else if (row._2 == "LiveVisit" && row._3 == "视频直播播放" && lcsIDList.contains(row._4))
        (row._1, 0, 0, 0, 0, 1, 0)
      else if (row._2 == "NativeAppVisit" && row._3 == "新闻详情页访问")
        (row._1, 0, 0, 0, 0, 0, 1)
      else
        (row._1, 0, 0, 0, 0, 0, 0)
    })

    //对UserID数据过滤
    val userDF: Dataset[(String, Int, Int, Int, Int, Int, Int)] = dataFrame.
      map(row => {
        //若event不为null时，才做逻辑处理。防止event事件为null导致的空指针现象发生。
        if (row.getString(1) != null) {
          val properJson = JSON.parseObject(row.getString(1))
          val userID = if (properJson.getOrDefault("userID", "") != null)
            properJson.getOrDefault("userID", "") else ""
          val v1_element_content = if (properJson.getOrDefault("v1_element_content", "") != null)
            properJson.getOrDefault("v1_element_content", "") else ""
          val v1_lcs_id = if (properJson.getOrDefault("v1_lcs_id", "") != null)
            properJson.getOrDefault("v1_lcs_id", "") else ""
          (userID.toString, if (row.getString(0) != null) row.getString(0) else "", v1_element_content.toString,v1_lcs_id.toString)
        } else ("", "", "","")
      }).map(row => {
      val lcsIDList: Set[String] = lcsIdBC.value
      if (row._2 == "NativeAppVisit" && row._3 == "TD合约详情页")
        (row._1, 1, 0, 0, 0, 0, 0)
      else if (row._2 == "NativeAppClick" && row._3 == "行情_T+D")
        (row._1, 0, 1, 0, 0, 0, 0)
      else if (row._2 == "H5Visit" && row._3 == "TD开户页")
        (row._1, 0, 0, 1, 0, 0, 0)
      else if (row._2 == "NativeAppVisit" && row._3 == "理财师主页访问" && lcsIDList.contains(row._4))
        (row._1, 0, 0, 0, 1, 0, 0)
      else if (row._2 == "LiveVisit" && row._3 == "视频直播播放" && lcsIDList.contains(row._4))
        (row._1, 0, 0, 0, 0, 1, 0)
      else if (row._2 == "NativeAppVisit" && row._3 == "新闻详情页访问")
        (row._1, 0, 0, 0, 0, 0, 1)
      else
        (row._1, 0, 0, 0, 0, 0, 0)
    })

    //Union聚合 ，三份流合到一起
    val devAndUserDF = deviceDF1.union(deviceDF2).union(userDF)


    //decAndUserDF聚合
    val devAndUserSum = devAndUserDF.groupBy("_1").sum("_2", "_3", "_4", "_5", "_6", "_7").
      map(row => {
        val is_visit_contractVarieties = if (row.getLong(1) > 0) 1 else 0
        val is_visit_marketPrice = if (row.getLong(2) > 0) 1 else 0
        val is_visit_Account = if (row.getLong(3) > 0) 1 else 0
        val is_visit_lcsPage = if (row.getLong(4) > 0) 1 else 0
        val is_visit_tdLive = if (row.getLong(5) > 3) 1 else 0
        val is_visit_tdInformation = if (row.getLong(6) > 0) 1 else 0
        (row.getString(0), is_visit_contractVarieties, is_visit_marketPrice, is_visit_Account, is_visit_lcsPage, is_visit_tdLive, is_visit_tdInformation)
      })


    //结果数据写入hive仓库
    devAndUserSum.createOrReplaceGlobalTempView("tempData")
    spark.sql(s"insert into dws.dws_td_user_lyering_label_1d partition(dt='$dt')" +
      " select * from global_temp.tempData")

  }

}
