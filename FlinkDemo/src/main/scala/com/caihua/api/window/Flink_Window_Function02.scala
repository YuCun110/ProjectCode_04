package com.caihua.api.window

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Window_Function02 {
  def main(args: Array[String]): Unit = {
    // TODO 自定义Window可以分成两类：
    //  1）增量聚合函数（incremental aggregation functions）：每条数据到来就进行计算，保持一个简单的状态
    //  2）全窗口函数（full window functions）：先把窗口所有数据收集起来，等到计算的时候会遍历所有数据

    //TODO 增量聚合函数：AggregateFunction

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.从端口读取数据，创建DS
    val portDS: DataStream[String] = envStream.socketTextStream("hadoop202", 9999)

    //3.扁平化
    val wordCountKS: KeyedStream[(String, Int), String] = portDS.flatMap(_.split(",")).map((_, 1)).keyBy(_._1)

    //4.每10秒开一次窗，并采用增量的方式聚合，求平均数
    // TODO 滚动窗口
    val resultDS: DataStream[(String, Int)] = wordCountKS.timeWindow(Time.seconds(10)).aggregate(
      // TODO 类似于Spark中的累加器
      new AggregateFunction[(String, Int), (Int, Int), (String, Int)] {
        //① 初始值
        override def createAccumulator() = (0, 0)

        //② “分区内”操作
        override def add(in: (String, Int), acc: (Int, Int)) = {
          (in._2 + acc._1, acc._2 + 1)
        }

        //③ “分区间”操作
        override def merge(acc: (Int, Int), acc1: (Int, Int)) = {
          (acc._1 + acc1._1, acc._2 + acc1._2)
        }

        //④ 返回结果
        override def getResult(acc: (Int, Int)) = {
          ("Sensor", (acc._1 / acc._2))
        }
      }
    )

    //5.打印输出
    resultDS.print()

    //6.执行任务
    envStream.execute()
  }
}
