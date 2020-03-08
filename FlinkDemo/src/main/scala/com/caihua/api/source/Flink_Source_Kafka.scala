package com.caihua.api.source

import java.util.Properties

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Source_Kafka {
  def main(args: Array[String]): Unit = {
    // TODO 从Kafka中读取数据
    //1.创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读取Kafka中的数据，创建DataStream
    //① 设置Topic
    val topic = "flink_source"
    //② 创建配置信息
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092,hadoop203:9092,hadoop204:9092")
    properties.setProperty("group.id", "group01")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")
    //③ 导入隐式转换
    import org.apache.flink.api.scala._
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties))

    //3.将数据封装为样例类
    val waterSensorDS: DataStream[WaterSensor] = kafkaDS.map(str => {
      val arr: Array[String] = str.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //4.打印输出
    waterSensorDS.print()

    //5.启动任务
    env.execute()
  }
}
