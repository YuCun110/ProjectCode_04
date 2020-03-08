package com.caihua.api.watermark

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Watermark_EventTime_In_Window {
  def main(args: Array[String]): Unit = {
    // TODO EvnetTime在window中的使用
    //  1.滚动窗口（TumblingEventTimeWindows）
    //  2.滑动窗口（SlidingEventTimeWindows）
    //  3.会话窗口（EventTimeSessionWindows）：相邻两次数据的EventTime的时间差超过指定的时间间隔就会触发执行

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

    // TODO assignAscendingTimestamps：直接使用数据的时间戳生成watermark
    //5.抽取事件数据的时间戳，并设置水位线
    val markDS: DataStream[WaterSensor] = waterSensorDS.assignAscendingTimestamps(_.ts * 1000)

    //6.分流，开窗
    val applyDS: DataStream[String] = markDS
      .keyBy(_.id)
      // TODO 1.设定EventTime的滚动窗口
      //.timeWindow(Time.seconds(5))
      //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
      // TODO 2.设定EventTime的滑动窗口
      //.timeWindow(Time.seconds(5),Time.seconds(2))
      //.window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(2)))
      // TODO 3.设定EventTime的会话窗口
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
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
