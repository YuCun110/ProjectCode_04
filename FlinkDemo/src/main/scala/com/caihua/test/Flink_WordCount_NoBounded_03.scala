package com.caihua.test

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_WordCount_NoBounded_03 {
  def main(args: Array[String]): Unit = {
    // TODO 使用无解流的方式--统计WordCount
    //1.创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读取端口数据，创建DS
    val wordsDS: DataStream[String] = env.socketTextStream("hadoop202",9999)

    //3.扁平化：拆分单词
    import org.apache.flink.api.scala._
    val wordToOneDS: DataStream[String] = wordsDS.flatMap(_.split(" "))

    //4.变换数据结构
    val wordToTupleDS: DataStream[(String, Int)] = wordToOneDS.map((_,1))

    //5.分组：按照Key进行分组
    val groupByKeyKS: KeyedStream[(String, Int), String] = wordToTupleDS.keyBy(_._1)

    //6.聚合：统计单词出现的次数
    val wordCountDS: DataStream[(String, Int)] = groupByKeyKS.sum(1)

    //7.打印输出
    wordCountDS.print("sum = ")

    //8.启动任务
    env.execute()
  }

}
