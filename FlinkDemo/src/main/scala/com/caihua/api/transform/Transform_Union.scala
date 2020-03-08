package com.caihua.api.transform

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Transform_Union {
  def main(args: Array[String]): Unit = {
    // TODO Union：对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream。

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.读取数据，创建DS
    val waterSensor: DataStream[WaterSensor] = envStream.fromCollection(Seq(WaterSensor("1001", System.currentTimeMillis(), Random.nextInt(35)),
      WaterSensor("1002", System.currentTimeMillis(), Random.nextInt(60)),
      WaterSensor("1003", System.currentTimeMillis(), Random.nextInt(30))))

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

    //5.将三种不同日志类型的数据联合
    // TODO　Union
    val unionDS: DataStream[WaterSensor] = normalDS.union(warnDS,alarmDS)

    //6.打印输出
    unionDS.print()

    //7.执行任务
    envStream.execute()
  }
}
