package com.caihua.api.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_CountWindow {
  def main(args: Array[String]): Unit = {
    // TODO Window可以分成两类：
    //  1）CountWindow：按照指定的数据条数生成一个Window，与时间无关；
    //  2）TimeWindow：按照时间生成Window

    //TODO CountWindow：根据窗口中相同key元素的数量来触发执行

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.从端口读取数据，创建DS
    val portDS: DataStream[String] = envStream.socketTextStream("hadoop202", 9999)

    //3.扁平化
    val wordCountKS: KeyedStream[(String, Int), String] = portDS.flatMap(_.split(",")).map((_, 1)).keyBy(_._1)

    //4.每三个数据开一次窗
    // TODO 滚动窗口
    val windowDS: DataStream[(String, Int)] = wordCountKS.countWindow(3).reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )

    //5.打印输出
    windowDS.print()

    //6.执行任务
    envStream.execute()
  }
}
