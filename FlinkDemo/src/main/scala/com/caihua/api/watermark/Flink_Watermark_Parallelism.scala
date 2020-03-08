package com.caihua.api.watermark

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Watermark_Parallelism {
  def main(args: Array[String]): Unit = {
    // TODO Watermark是一种衡量Event Time进展的机制，用于处理乱序事件的，而正确的处理乱序事件，通常用Watermark机制结合window来实现

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //TODO Ⅰ.并行度设置：如果并行度不为1，那么在计算窗口时，是按照不同并行度单独计算的。
    // 此时watermark是跨分区的，多个分区的watermark通过广播方式传递，
    // 会出现一个分区拿到不同分区的watermark，最终会选择较小的watermark使用
    // ，并且只会在一端输出
    //设置并行度
    envStream.setParallelism(2)

    //2.设置时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.从端口读取数据，并创建DS
    val socketDS: DataStream[String] = envStream.socketTextStream("hadoop202", 9999)

    //4.转换数据结构：将数据拆分，封装为样例类对象
    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // TODO Ⅱ.指定时间戳，设置水位线：assignTimestampsAndWatermarks
    //  1. 从数据中抽取数据作为事件时间
    //  2. 设定水位线标记，这个标记一般比当前数据事件时间要推迟
    //5.抽取时间戳和设定水位线
    val watermarkDS: DataStream[WaterSensor] = waterSensorDS.assignTimestampsAndWatermarks(
      //① 设置水位线：BoundedOutOfOrdernessTimestampExtractor[T](Time)
      new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
        override def extractTimestamp(element: WaterSensor) = {
          //② 抽取时间的时间戳（毫秒）
          element.ts * 1000L
        }
      }
    )

    //6.分流：按id进行分流
    val applyDS: DataStream[String] = watermarkDS
      .keyBy(_.id)
      .timeWindow(Time.seconds(5)) //开窗
      .apply( //对窗口数据进行处理
        //key : 分流的key
        //window : 当前使用的窗口类型
        //datas : 窗口中的数据
        //out : 输出[输出数据的类型]
        (key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
          //① 获取窗口开始时间
          val start: Long = window.getStart
          //② 获取窗口结束时间
          val end: Long = window.getEnd
          //③ 输出数据
          out.collect(s"[$start-$end)，数据：[$datas]")
        }
      )

    //7.打印输出
    watermarkDS.print("mark--")
    applyDS.print("apply--")

    //8.启动任务
    envStream.execute()
  }
}
