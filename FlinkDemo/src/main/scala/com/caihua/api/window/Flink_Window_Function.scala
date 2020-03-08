package com.caihua.api.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Window_Function {
  def main(args: Array[String]): Unit = {
    // TODO 自定义Window可以分成两类：
    //  1）增量聚合函数（incremental aggregation functions）：每条数据到来就进行计算，保持一个简单的状态
    //  2）全窗口函数（full window functions）：先把窗口所有数据收集起来，等到计算的时候会遍历所有数据

    //TODO 增量聚合函数：ReduceFunction

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.从端口读取数据，创建DS
    val portDS: DataStream[String] = envStream.socketTextStream("hadoop202", 9999)

    //3.扁平化
    val wordCountKS: KeyedStream[(String, Int), String] = portDS.flatMap(_.split(",")).map((_, 1)).keyBy(_._1)

    //4.每10秒开一次窗，并采用增量的方式聚合
    // TODO 滚动窗口
    val windowDS: DataStream[(String, Int)] = wordCountKS.timeWindow(Time.seconds(10)).reduce(
      new ReduceFunction[(String, Int)] {
        override def reduce(t1: (String, Int), t2: (String, Int)) = {
          println(t1._1 +  "计算一次：" + (t1._2 + t2._2))
          (t1._1, t1._2 + t2._2)
        }
      }
    )

    //5.打印输出
    windowDS.print()

    //6.执行任务
    envStream.execute()
  }
}
