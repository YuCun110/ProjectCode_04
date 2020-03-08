package com.caihua.api.transform

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Transform_Connect_CoMap {
  def main(args: Array[String]): Unit = {
    // TODO 连接操作 Connect：连接两个保持他们类型的数据流，两个数据流被Connect之后，只是被放在了一个同一个流中，
    //  内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。

    // TODO 分别map操作 CoMap：作用于ConnectedStreams上，功能与map和flatMap一样
    //  ，对ConnectedStreams中的每一个Stream分别进行map和flatMap处理。
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.读取数据，创建DS
    val waterSensor: DataStream[WaterSensor] = envStream.fromCollection(Seq(WaterSensor("1001", System.currentTimeMillis(), Random.nextInt(35)),
      WaterSensor("1001", System.currentTimeMillis(), Random.nextInt(60)),
      WaterSensor("1001", System.currentTimeMillis(), Random.nextInt(30))))

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
    //val normalDS: DataStream[WaterSensor] = splitDS.select("normal")
    val warnDS: DataStream[WaterSensor] = splitDS.select("warn")
    val alarmDS: DataStream[WaterSensor] = splitDS.select("alarm")

    //5.将warn和alarm级别的数据流放入一个流中
    // TODO Connect
    val connectStream: ConnectedStreams[WaterSensor, WaterSensor] = warnDS.connect(alarmDS)

    //6.分别对warn和alarm，添加警告信息
    // TODO CoMap
    val coMapDS: DataStream[(String, String)] = connectStream.map(
      waterSensor1 => {
        (waterSensor1.toString, "warn")
      },
      waterSensor2 => {
        (waterSensor2.toString, "alarm")
      }
    )

    //7.打印输出
    coMapDS.print()

    //8.执行任务
    envStream.execute()
  }

}
