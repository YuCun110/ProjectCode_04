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
object Flink_Watermark_Lateness {
  def main(args: Array[String]): Unit = {
    // TODO Watermark是一种衡量Event Time进展的机制，用于处理乱序事件的，而正确的处理乱序事件，通常用Watermark机制结合window来实现

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //TODO Ⅰ.并行度设置
    //设置并行度
    envStream.setParallelism(1)

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

    // TODO Ⅲ.对窗口结算完毕后的迟到数据进行处理：延迟窗口的计算
    //  1.如果watermark已经触发了窗口的计算，那么这个窗口就不会再接收数据
    //  2.延迟窗口计算，接收延迟的数据：allowedLateness(Time)
    //6.分流：按id进行分流
    val applyDS: DataStream[String] = watermarkDS
      .keyBy(_.id)
      .timeWindow(Time.seconds(5)) //开窗
      .allowedLateness(Time.seconds(2)) //延迟
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
