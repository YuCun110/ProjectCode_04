package com.caihua.api.transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Transform_RollingAggregation {
  def main(args: Array[String]): Unit = {
    // TODO 滚动聚合算子Rolling Aggregation：每一个支流做聚合
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.读取数据，创建DS
    val linesDS: DataStream[String] = envStream.readTextFile("input")

    //3.扁平化：拆分单词
    //导入隐式转换
    val wordToOneDS: DataStream[String] = linesDS.flatMap(_.split(" "))

    //4.变换数据类型
    val tupleDS: DataStream[(String, Int)] = wordToOneDS.map((_,1))

    //5.按照单词进行分组
    val groupByKeyKS: KeyedStream[(String, Int), String] = tupleDS.keyBy(_._1)

    //6.聚合
    // TODO sum min max
    val sumDS: DataStream[(String, Int)] = groupByKeyKS.sum(1)
    val minDS: DataStream[(String, Int)] = groupByKeyKS.min(1)
    val maxDS: DataStream[(String, Int)] = groupByKeyKS.max(1)

    //7.打印输出
    sumDS.print("sum=")
    minDS.print("min=")
    maxDS.print("max=")

    //8.执行任务
    envStream.execute()
  }
}
