package com.caihua.api.source

import com.caihua.bean.WaterSensor
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Source_DefSource {
  def main(args: Array[String]): Unit = {
    // TODO 自定义数据源：自定义类继承SourceFunction[T]，实现run、cancel方法

    //1.创建上下文对象
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.从自定义数据源读取数据，创建DS
    val mySourceDS: DataStreamSource[WaterSensor] = envStream.addSource(new MySource)

    //3.打印输出
    mySourceDS.print()

    //4.启动任务
    envStream.execute()
  }
}

class MySource extends SourceFunction[WaterSensor]{
  //定义标记，是否读取数据
  var isRead = true

  //1.开始读取数据
  override def run(sourceContext: SourceFunction.SourceContext[WaterSensor]): Unit = {
    while (isRead){
      //① 生成随机数据
      val waterSensor = new WaterSensor("10001",System.currentTimeMillis(),new Random().nextDouble()*10)
      //② 将数据发送
      sourceContext.collect(waterSensor)
      //③ 模拟间歇
      Thread.sleep(300)
    }
  }

  //2.关闭读取数据
  override def cancel(): Unit = {
    isRead = false
  }
}
