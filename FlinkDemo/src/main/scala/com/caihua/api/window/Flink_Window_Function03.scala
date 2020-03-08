package com.caihua.api.window

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Window_Function03 {
  def main(args: Array[String]): Unit = {
    // TODO 自定义Window可以分成两类：
    //  1）增量聚合函数（incremental aggregation functions）：每条数据到来就进行计算，保持一个简单的状态
    //  2）全窗口函数（full window functions）：先把窗口所有数据收集起来，等到计算的时候会遍历所有数据

    //TODO 全窗口函数：ProcessWindowFunction

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.从端口读取数据，创建DS
    val portDS: DataStream[String] = envStream.socketTextStream("hadoop202", 9999)

    //3.扁平化
    val wordCountKS: KeyedStream[(String, Int), String] = portDS.flatMap(_.split(",")).map((_, 1)).keyBy(_._1)

    //4.每10秒开一次窗，并采用增量的方式聚合，求平均数
    // TODO 滚动窗口
    val processDS: DataStream[String] = wordCountKS.timeWindow(Time.seconds(10)).process(
      new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
        override def process(key: String, //数据的Key
                             context: Context, //上下文环境
                             elements: Iterable[(String, Int)], //窗口中所有相同Key的数据
                             out: Collector[String] //收集器，用于将数据输出到Sink
                            ): Unit = {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          out.collect("窗口启动时间：" + sdf.format(new Date(context.window.getStart)))
          out.collect("窗口结束时间:" + sdf.format(new Date(context.window.getEnd)))
          out.collect("计算的数据为 ：" + elements.toList)
        }
      }
    )


    //5.打印输出
    processDS.print()

    //6.执行任务
    envStream.execute()
  }
}
