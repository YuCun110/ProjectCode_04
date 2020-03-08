package com.caihua.api.transform

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Transform_Reduce {
  def main(args: Array[String]): Unit = {
    // TODO 分组流的聚合操作 Reduce：合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果
    //  而不是只返回最后一次聚合的最终结果
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.读取数据，创建DS
    val linesDS: DataStream[String] = envStream.readTextFile("input")

    //3.扁平化：拆分单词
    //导入隐式转换
    val wordToOneDS: DataStream[String] = linesDS.flatMap(_.split(" "))

    //4.变换数据类型
    val tupleDS: DataStream[(String, Int)] = wordToOneDS.map((_, 1))

    //5.按照单词进行分组
    val groupByKeyKS: KeyedStream[(String, Int), String] = tupleDS.keyBy(_._1)

    //6.按照单词进行聚合
    // TODO Reduce
    val reduceDS: DataStream[(String, Int)] = groupByKeyKS.reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )

    //7.打印输出
    reduceDS.print()

    //8.执行任务
    envStream.execute()
  }

}
