package com.caihua.api.source

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Source_Collection {
  def main(args: Array[String]): Unit = {
    //1.创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.从集合读取数据，创建DS
    //① 导入隐式转换
    import org.apache.flink.api.scala._
    //② 读取数据
    val collectionDS: DataStream[(String, Int, Double)] = env.fromCollection(List(("ws_001",1577844001,45.0),("ws_002",1577844002,55.0),("ws_003",1577844003,37.0)))

    //3.将数据封装为样例类
    val waterSensorDS: DataStream[WaterSensor] = collectionDS.map(tuple => WaterSensor(tuple._1,tuple._2,tuple._3))

    //4.打印输出
    waterSensorDS.print()

    //5.启动任务
    env.execute()
  }
}
