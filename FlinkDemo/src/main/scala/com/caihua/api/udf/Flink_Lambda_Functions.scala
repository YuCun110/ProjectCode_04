package com.caihua.api.udf

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_FilterFunction {
  def main(args: Array[String]): Unit = {
    // TODO 匿名函数 Lambda Function
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(2)

    //2.从文件读取数据，创建DS
    val linesDS: DataStream[String] = envStream.readTextFile("input/ws.txt")

    //3.将行数据用逗号拆分
    // TODO 匿名函数
    val datas: DataStream[String] = linesDS.flatMap(_.split(","))

    //4.打印输出
    datas.print()

    //5.执行任务
    envStream.execute()
  }
}

