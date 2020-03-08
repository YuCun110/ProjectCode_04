package com.caihua.api.transform

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Transform_Split_Select {
  def main(args: Array[String]): Unit = {
    // TODO 拆分操作 Split：根据某些特征把一个DataStream拆分成两个或者多个DataStream
    // TODO 选择操作 Select：从一个SplitStream中获取一个或者多个DataStream
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.读取数据，创建DS
    val waterSensor: DataStream[WaterSensor] = envStream.fromCollection(Seq(WaterSensor("1001", System.currentTimeMillis(), Random.nextInt(35)),
      WaterSensor("1001", System.currentTimeMillis(), Random.nextInt(35)),
      WaterSensor("1001", System.currentTimeMillis(), Random.nextInt(35))))

    //3.分流：按照水位高低（以40cm,30cm为界）拆分成三个流
    // TODO　Split
    val splitDS: SplitStream[WaterSensor] = waterSensor.split(waterSensor => {
      if (waterSensor.vc < 30) {
        Seq("normal")
      } else if (waterSensor.vc < 40) {
        Seq("warn")
      } else {
        Seq("alarm")
      }
    })

    //4.按照警告级别，分级打印
    // TODO Select
    val normalDS: DataStream[WaterSensor] = splitDS.select("normal")
    val warnDS: DataStream[WaterSensor] = splitDS.select("warn")
    val alarmDS: DataStream[WaterSensor] = splitDS.select("alarm")

    //5.打印输出
    normalDS.print("normal: ")
    warnDS.print("warn: ")
    alarmDS.print("alarm: ")

    //6.执行任务
    envStream.execute()
  }

}
