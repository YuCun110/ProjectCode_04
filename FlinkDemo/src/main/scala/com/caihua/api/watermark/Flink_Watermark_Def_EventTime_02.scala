package com.caihua.api.watermark

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Watermark_Def_EventTime_02 {
  def main(args: Array[String]): Unit = {
    // TODO 自定义从事件数据中抽取时间：
    //  TimestampAssigner 有两种类型
    //  1.AssignerWithPeriodicWatermarks：周期性的生成watermark
    //  2.AssignerWithPunctuatedWatermarks：间断式地生成watermark
    //1.创建上下文日志环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(1)

    //2.定义时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // TODO 可自定义水位线生成的周期
    //envStream.getConfig.setAutoWatermarkInterval(500)

    //3.从端口读取数据，创建DS
    val socketDS: DataStream[String] = envStream.socketTextStream("hadoop202",9999)

    //4.变换数据结构，将数据拆分，封装为样例类对象
    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // TODO AssignerWithPunctuatedWatermarks
    //  这种方式不是固定时间的，而是可以根据需要对每条数据进行筛选和处理
    //5.抽取事件数据的时间戳，并设置水位线
    val markDS: DataStream[WaterSensor] = waterSensorDS.assignTimestampsAndWatermarks(
      //① 自定义事件时间的抽取
      new AssignerWithPunctuatedWatermarks[WaterSensor] {
        //间歇性的生成水位线
        override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long) = {
          println("checkAndGetNextWatermark--------------")
          new Watermark(extractedTimestamp)
        }

        //抽取事件时间
        override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long) = {
          element.ts * 1000L
        }
      }
    )

    //6.分流，开窗
    val applyDS: DataStream[String] = markDS
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(
        (key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
          out.collect(s"[${window.getStart} - ${window.getEnd},数据：[$datas])")
        }
      )

    //7.打印输出
    markDS.print("mark--")
    applyDS.print("apply--")

    //8.启动任务
    envStream.execute()
  }
}
