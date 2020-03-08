package com.caihua.test

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_WordCount_Bounded_02 {
  def main(args: Array[String]): Unit = {
    // TODO 使用有界流的方式--统计WordCount
    //1.创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(2)

    //2.读取文件数据，创建DS
    val linesDS: DataStream[String] = env.readTextFile("input")

    //3.扁平化：拆分单词
    // TODO 导入隐式转换（将字符串转换为输入流）：org.apache.flink.api.scala.DataSet[R]
    import org.apache.flink.api.scala._
    val wordToOneDS: DataStream[String] = linesDS.flatMap(_.split(" "))

    //4.转换数据结构：word -> (word,1)
    val wordToTupleDS: DataStream[(String, Int)] = wordToOneDS.map((_,1))

    //5.按照Key进行分组
    val groupByKeyKS: KeyedStream[(String, Int), String] = wordToTupleDS.keyBy(_._1)

    //6.聚合：统计单词个数
    val wordCountDS: DataStream[(String, Int)] = groupByKeyKS.sum(1)

    //7.结果打印
    wordCountDS.print()

    //8.启动流处理（开启采集数据）
    env.execute("Flink_WordCount_Bounded_02")
  }

}
