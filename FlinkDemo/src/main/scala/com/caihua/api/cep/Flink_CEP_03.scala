package com.caihua.api.cep

import com.caihua.bean.WaterSensor
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_CEP_03 {
  def main(args: Array[String]): Unit = {
    // TODO CEP：模式序列
    //  1.严格近邻（Strict Contiguity）：所有事件按照严格的顺序出现，中间没有任何不匹配的事件，由 .next() 指定
    //  2.宽松近邻（ Relaxed Contiguity ）：允许中间出现不匹配的事件，由 .followedBy() 指定
    //  3.非确定性宽松近邻（ Non-Deterministic Relaxed Contiguity ）：进一步放宽条件，之前已经匹配过的事件也可以再次使用，由 .followedByAny() 指定
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //2.定义时间语义
    //envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.从本地文件读取数据，创建DS
    val dataDS: DataStream[String] = envStream.readTextFile("input/ws.txt")

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
      .where(_.id == "1001")
      // TODO 严格近邻
      //.next("next")
      // TODO 宽松近邻
      //.followedBy("follow")
      // TODO 非确定性宽松近邻
      .followedByAny("followAny")
      .where(_.id == "1003")
       // TODO 指定时间约束
      .within(Time.seconds(2))
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
