package cn.yintech.utils

import java.util.Calendar
import java.text.SimpleDateFormat

import cn.yintech.hbase.HbaseUtilsScala.{getHbaseConf, scaneByPrefixFilter}
import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory

import scala.collection.mutable.ListBuffer

object ScalaUtils {

  def main(args: Array[String]): Unit = {

    val startTime = "2021-02-18 19:15:31"
    val endTime = "2021-02-18 21:17:09"
    val uid = "26991571"
    val projId = "168"

    val conn = ConnectionFactory.createConnection(getHbaseConf)
    val table = TableName.valueOf("caishang_course_viewing_record")
    val htable = conn.getTable(table)

    val hbaseRecords = scaneByPrefixFilter(htable, uid.reverse + "|" + projId + "|prod")
    val tuples = hbaseRecords.map(v => {
      val splits = v.split("\\|")
      (splits(3), splits(4))
    })
    val halfMinutes = getBetweenHalfMinute(startTime, endTime)
    val live = tuples.filter(_._1.equals("live")).map(v => halfMinutes.indexOf(v._2)).filter(_ != -1)
    val play = tuples.filter(_._1.equals("play")).map(_._2.toInt / 30)
    val proceding = live.union(play).toSet
    val halfMinutesPoint = halfMinutes.indices

    println(play)
    println(halfMinutes)

    val intersect = halfMinutesPoint.toSet.intersect(proceding) // 交集，以观看
    val diff = halfMinutesPoint.toSet.diff(proceding) // 差集，未观看
    println(intersect.size * 1.0 / halfMinutesPoint.size)
    println(intersect.size * 30)


  }

  def getBetweenHalfMinute(start: String, end: String): Seq[String] = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startData = sdf.parse(start); //定义起始日期
    startData.setTime(startData.getTime / 1000 / 30 * 1000 * 30 + 30000)
    val endData = sdf.parse(end); //定义结束日期
    var buffer = new ListBuffer[String]

    val tempStart = Calendar.getInstance()
    tempStart.setTime(startData)
    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)

    buffer += tempStart.getTimeInMillis + ""
    while (!tempStart.after(tempEnd)) {
      //      buffer += sdf.format(tempStart.getTime)
      buffer += tempStart.getTimeInMillis + ""
      tempStart.add(Calendar.SECOND, 30)
    }
    buffer.distinct.toList
  }

  def jsonParse(value: String): Map[String, String] = {
    var map = Map[String, String]()
    val jsonParser = new JSONParser()
    try {
      val outJsonObj: JSONObject = jsonParser.parse(value).asInstanceOf[JSONObject]
      val outJsonKey = outJsonObj.keySet()
      val outIter = outJsonKey.iterator

      while (outIter.hasNext) {
        val outKey = outIter.next()
        val outValue = if (outJsonObj.get(outKey) != null) outJsonObj.get(outKey).toString else "null"
        map += (outKey -> outValue)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    map
  }
}
