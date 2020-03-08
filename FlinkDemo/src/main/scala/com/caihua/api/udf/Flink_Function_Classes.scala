package com.caihua.api.udf

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Function_Classes {
  def main(args: Array[String]): Unit = {
    // TODO 自定义映射转换函数（MapFunction,FilterFunction,ProcessFunction）
    // TODO 1.MapFunction
    // TODO 继承（实现）MapFunction接口，并定义泛型（输入，输出）；实现方法；
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.从文件读取数据，创建DS
    val linesDS: DataStream[String] = envStream.readTextFile("input/ws.txt")

    //3.自定义映射函数，将行数据用逗号拆分，并封装为样例类对象
    // TODO MapFunction
    //导入隐式转换
    val waterSensorDS: DataStream[WaterSensor] = linesDS.map(new MyMapFunction)

    //4.打印输出
    waterSensorDS.print()

    //5.执行任务
    envStream.execute()
  }
}

class MyMapFunction extends MapFunction[String, WaterSensor] {
  override def map(t: String): WaterSensor = {
    val arr: Array[String] = t.split(",")
    WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
  }
}
