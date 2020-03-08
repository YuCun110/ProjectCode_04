package com.caihua.api.state

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_State_OperatorState {
  def main(args: Array[String]): Unit = {
    // TODO 算子状态：作用范围限定为算子任务
    //  三种基本数据类型：1）列表状态（List state）；2）联合列表状态（Union list state）；3）广播状态（Broadcast state）

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //2.定义时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.读取端口数据，并创建DS
    val socketDS: DataStream[String] = envStream.socketTextStream("hadoop202",9999)

    //4.变换数据结构
    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //5.抽取事件时间戳，并设置水位线(Watermark = EventTime - 1ms)
    val markDS: DataStream[WaterSensor] = waterSensorDS.assignAscendingTimestamps(_.ts * 1000)

    //6.按照id进行分组
    val keyByDS: KeyedStream[WaterSensor, String] = markDS.keyBy(_.id)

    // TODO 算子状态
    //7.利用算子状态，对累计水位值进行求和
    val sumDS: DataStream[WaterSensor] = keyByDS.mapWithState(
      (ws, op:Option[Double]) => {
        //累加
        val tmp = ws.vc + op.getOrElse(0.0)
        println("累计水位：" + tmp)
        (ws, Option(tmp))
      }
    )
    //keyByDS.flatMapWithState()
    //keyByDS.filterWithState()

    //8.打印输出
    sumDS.print("sum--")

    //9.启动任务
    envStream.execute()
  }
}
