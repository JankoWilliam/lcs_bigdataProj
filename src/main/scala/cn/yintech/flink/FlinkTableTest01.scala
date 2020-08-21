package cn.yintech.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

object FlinkTableTest01 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count,
      'word)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count,
      'word)
    val table = table1
      .where('word.like("F%"))
      .unionAll(table2)
    val explanation: String = tEnv.explain(table)
    println(explanation)
  }
}
