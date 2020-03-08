package com.caihua.api.udf

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Rich_Function {
  def main(args: Array[String]): Unit = {
    // TODO 富函数（RichMapFunction,RichFilterFunction  ）：Rich Function有一个生命周期的概念
    // TODO 典型的生命周期方法有：
    //  1）open()方法是rich function的初始化方法；
    //  2）close()方法是生命周期中的最后一个调用的方法；
    //  3）	getRuntimeContext()方法提供了函数的执行的并行度，任务的名字，以及state状态
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.从文件读取数据，创建DS
    val linesDS: DataStream[String] = envStream.readTextFile("input/ws.txt")

    //3.自定义映射函数，将行数据用逗号拆分，并封装为样例类对象
    // TODO RichMapFunction
    //导入隐式转换
    val waterSensorDS: DataStream[(Int, WaterSensor)] = linesDS.map(new MyRichMap)

    //4.打印输出
    waterSensorDS.print()

    //5.执行任务
    envStream.execute()
  }
}

class MyRichMap extends RichMapFunction[String, (Int,WaterSensor)] {
  override def close(): Unit = super.close()

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def map(t: String): (Int, WaterSensor) = {
    val arr: Array[String] = t.split(",")
    (this.getRuntimeContext.getIndexOfThisSubtask,WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble))
  }
}
