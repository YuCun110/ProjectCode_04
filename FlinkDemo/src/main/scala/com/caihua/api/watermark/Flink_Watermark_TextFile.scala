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
object Flink_Watermark_TextFile {
  def main(args: Array[String]): Unit = {
    // TODO Watermark是一种衡量Event Time进展的机制

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //TODO Ⅰ.并行度设置：如果并行度不为1，那么在计算窗口时，是按照不同并行度单独计算的。

    //设置并行度
    envStream.setParallelism(1)

    //2.设置时间语义
    // TODO 默认会每200毫秒来生成watermark：getConfig().setAutoWatermarkInterval(200);
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // TODO Ⅴ.从本地文件读取数据
    //  1.文件读取结束进行计算时，即使窗口没有提交，也会进行结算
    //  2.文件读取结束后，Flink会将watermark设置为Long的最大值
    //  3.将所有未关闭计算的窗口立即计算
    //3.从本地文件读取数据，并创建DS
    val socketDS: DataStream[String] = envStream.readTextFile("input/sensor-data.log")

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

    //6.创建侧输出流，并设置id
    val outputTag = new OutputTag[WaterSensor]("lateData")

    //7.分流：按id进行分流
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

    //8.打印输出
    watermarkDS.print("mark--")
    applyDS.print("apply--")

    //9.启动任务
    envStream.execute()
  }
}
