package com.caihua.api.processfunction

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_ProcessFunction_Keyed {
  def main(args: Array[String]): Unit = {
    // TODO KeyedProcessFunction：会处理流的每一个元素，输出为0个、1个或者多个元素
    //  提供了两个方法：
    //  1）processElement(v: IN, ctx: Context, out: Collector[OUT])：流中的每一个元素都会调用这个方法，调用结果将会放在Collector数据类型中输出
    //  2）onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT])是一个回调函数。当之前注册的定时器触发时调用
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //2.定义时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.从端口读取数据，创建DS
    val lineDS: DataStream[String] = envStream.socketTextStream("hadoop202",9999)

    //4.变换数据结构，拆分数据，并封装为样例类对象
    val waterSensorDS: DataStream[WaterSensor] = lineDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //5.按照id进行分流
    val keyByKS: KeyedStream[WaterSensor, String] = waterSensorDS.keyBy(_.id)

    //6.遍历每一个元素，执行process
    val processDS: DataStream[String] = keyByKS.process(
      // TODO KeyedProcessFunction[KEY,IN,OUT]
      new KeyedProcessFunction[String, WaterSensor, String] {
        //① 每来一条数据，就执行一次该方法
        override def processElement(value: WaterSensor, //输出的参数
                                    ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, //上下文环境
                                    out: Collector[String] //输出的数据集
                                   ) = {
          //ctx.getCurrentKey //获取当前数据的Key
          //ctx.output() //采集数据到侧输出流
          //ctx.timestamp() //事件的时间戳
          //ctx.timerService() //和时间相关的服务：定时器
          //getRuntimeContext //运行环境

          //输出
          out.collect("keyedProcess = " + ctx.timestamp())
          //注册定时器
          ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())
        }

        //② 触发定时器的执行
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          //定时器在指定时间执行
          out.collect("timer --")
        }
      }
    )

    //7.打印输出
    processDS.print("process--")

    //8.启动任务
    envStream.execute()
  }
}
