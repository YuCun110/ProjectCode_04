package com.caihua.api.watermark

import java.text.SimpleDateFormat

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Watermark {
  def main(args: Array[String]): Unit = {
    // TODO Watermark : 水位线（水印）
    //  是对迟到数据处理的机制，但是watermark达到数据窗口的结束的时候，触发窗口计算
    //  当窗口时间到达时，本身应该触发计算，但是为了能够对迟到数据进行正确的处理
    //  需要将计算的时间点推迟，推迟到watermark标记到达时
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(1)

    //设置时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.从端口读取数据
    val linesDS: DataStream[String] = envStream.socketTextStream("hadoop202",9999)

    //3.变换数据类型，将行数据进行拆分，封装为样例类对象
    val waterSensorDS: DataStream[WaterSensor] = linesDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //4.抽取时间戳，标记水位线
    val markDS: DataStream[WaterSensor] = waterSensorDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
        //抽取时间，设置为毫秒
        override def extractTimestamp(element: WaterSensor) = {
          element.ts * 1000
        }
      }
    )
    markDS.print("mark--")

    //5.开窗
    val applyDS: DataStream[String] = markDS.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(
        (key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
          //val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          out.collect(s"[${window.getStart}-${window.getEnd}),数据：[$datas]")
        }
      )

    //6.打印输出

    applyDS.print("window--")

    //7.启动任务
    envStream.execute()
  }
}
