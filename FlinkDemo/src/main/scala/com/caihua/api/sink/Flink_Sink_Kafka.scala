package com.caihua.api.sink

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Sink_Kafka {
  def main(args: Array[String]): Unit = {
    // TODO Sink：将数据写入Kafka集群 FlinkKafkaProducer011
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.从集合中读取数据，创建DS
    //导入隐式转换
    val waterSensorDS: DataStream[WaterSensor] = envStream.fromCollection(
      Seq(WaterSensor("1001", System.currentTimeMillis(), Random.nextDouble()),
      WaterSensor("1001", System.currentTimeMillis(), Random.nextDouble()),
      WaterSensor("1001", System.currentTimeMillis(), Random.nextDouble())
    ))

    //3.转换数据结构，将样例类对象转换为JSON转换为字符串

    val stringDS: DataStream[String] = waterSensorDS.map(waterSensor => {
      //导入隐式转换，将样例类对象转换为JSON字符串
      import org.json4s.native.Serialization
      implicit val formats=org.json4s.DefaultFormats
      //写入Kafka
      Serialization.write(waterSensor)
    })

    //4.将数据写出Kafka
    stringDS.addSink(new FlinkKafkaProducer011[String](
      "hadoop202:9092",
      "flink_sink",
      new SimpleStringSchema()
    ))

    //5.启动任务
    envStream.execute()
  }
}
