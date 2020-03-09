package com.caihua.api.cep

import com.caihua.bean.WaterSensor
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_CEP_01 {
  def main(args: Array[String]): Unit = {
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //2.定义时间语义
    //envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.从本地文件读取数据，创建DS
    val dataDS: DataStream[String] = envStream.readTextFile("input/sensor-data.log")

    //4.变换数据结构：拆分数据，并封装为样例类对象
    val waterSensorDS: DataStream[WaterSensor] = dataDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(
        arr(0),
        arr(1).toLong,
        arr(2).toDouble
      )
    })

    //5.使用CEP
    // TODO Pattern API
    //① 定义Pattern
    val pattern= Pattern.begin[WaterSensor]("begin")
      // TODO where：增加新的条件，多个条件同时满足
      .where(_.vc <= 5)
      // TODO or：增加新的条件，多个条件满足任意一个
      .or(_.vc > 6)
    //② 应用规则
    val patternPS: PatternStream[WaterSensor] = CEP.pattern(waterSensorDS,pattern)
    //③ 获取处理结果
    val result: DataStream[String] = patternPS.select(
      map => {
        map.toString
      }
    )

    result.print("--")

    //6.启动任务
    envStream.execute()
  }
}
