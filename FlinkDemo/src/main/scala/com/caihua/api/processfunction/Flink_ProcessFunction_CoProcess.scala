package com.caihua.api.processfunction

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_ProcessFunction_CoProcess {
  def main(args: Array[String]): Unit = {
    // TODO CoProcessFunction：类似于ProcessFunction，用来处理两条流
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //2.定义时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.从端口读取数据，创建DS
    val socketDS: DataStream[String] = envStream.socketTextStream("hadoop202",9999)

    //4.变换数据结构，拆分数据，封装为样例类对象
    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //5.抽取事件时间戳，并设置水位线(watermark = eventTime - 1ms)
    //val markDS: DataStream[WaterSensor] = waterSensorDS.assignAscendingTimestamps(_.ts * 1000)

    //6.按照水位观测值，进行分流
    val splitDS: SplitStream[WaterSensor] = waterSensorDS.split(data => {
      if (data.vc < 4) {
        Seq("normal")
      } else if (data.vc < 8) {
        Seq("warn")
      } else {
        Seq("alarm")
      }
    })

    //7.取出流
    //val normalDS: DataStream[WaterSensor] = splitDS.select("normal")
    val warnDS: DataStream[WaterSensor] = splitDS.select("warn")
    val alarmDS: DataStream[WaterSensor] = splitDS.select("alarm")

    //8.将警告级别为warn和alarm的流联合
    val connectDS: ConnectedStreams[WaterSensor, WaterSensor] = warnDS.connect(alarmDS)

    // TODO CoProcessFunction<IN1, IN2, OUT>
    //9.分别处理两条流
    val processDS: DataStream[String] = connectDS.process(
      new CoProcessFunction[WaterSensor, WaterSensor, String] {
        override def processElement1(value: WaterSensor, ctx: CoProcessFunction[WaterSensor, WaterSensor, String]#Context, out: Collector[String]) = {
          out.collect("Warn水位数据：" + value.ts)
        }

        override def processElement2(value: WaterSensor, ctx: CoProcessFunction[WaterSensor, WaterSensor, String]#Context, out: Collector[String]) = {
          out.collect("Alarm水位数据：" + value.ts)
        }
      }
    )

    //10.打印输出
    processDS.print("process--")

    //11.启动任务
    envStream.execute()
  }

}
