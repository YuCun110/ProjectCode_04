package com.caihua.api.transform

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Transform_KeyBy {
  def main(args: Array[String]): Unit = {
    // TODO：KeyBy算子
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    envStream.setParallelism(1)

    //3.读取数据，创建DS
    val linesDS: DataStream[String] = envStream.readTextFile("input")

    //4.扁平化：拆分单词
    val wordToOneDS: DataStream[String] = linesDS.flatMap(_.split(","))

    //5.转换数据结构
    val wordToTupleDS: DataStream[(String, Int)] = wordToOneDS.map((_,1))

    //6.按照Key进行分组
    // TODO：Scala的方式
    val groupByKS01: KeyedStream[(String, Int), String] = wordToTupleDS.keyBy(_._1)
    val count01: DataStream[(String, Int)] = groupByKS01.sum(1)

    // TODO：Java的方式
    val groupByKS02: KeyedStream[(String, Int), Tuple] = wordToTupleDS.keyBy(0)
    val count02: DataStream[(String, Int)] = groupByKS02.sum(1)

    //7.打印输出
    count01.print("count1：")
    count02.print("count2：")

    //8.启动任务
    envStream.execute()
  }

}
