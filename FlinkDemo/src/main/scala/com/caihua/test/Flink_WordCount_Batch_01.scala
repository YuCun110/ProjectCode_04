package com.caihua.test

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_WordCount_Batch_01 {
  def main(args: Array[String]): Unit = {
    // TODO:使用批处理的方式--统计WordCount
    //1.创建上下文环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.读取数据，创建DataSet
    val linesDataSet: DataSet[String] = environment.readTextFile("/opt/module/flink-1.7.2/input")

    //3.扁平化，拆分单词
    // TODO 导入隐式转换（将字符串转换为输入流）：org.apache.flink.api.scala.DataSet[R]
    import org.apache.flink.api.scala._
    val wordToOneDataSet: DataSet[String] = linesDataSet.flatMap(_.split(" "))

    //4.转换数据结构：word -> (word,1)
    val wordToTupleDS: DataSet[(String, Int)] = wordToOneDataSet.map((_,1))

    //5.分组：按照Key进行分组
    val groupByKeyDS: GroupedDataSet[(String, Int)] = wordToTupleDS.groupBy(0)

    //6.统计单词数
    val wordCountDS: AggregateDataSet[(String, Int)] = groupByKeyDS.sum(1)

    //7.打印结果
    wordCountDS.print()
  }
}
