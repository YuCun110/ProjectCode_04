package com.caihua.api.processfunction

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 *          监控水位传感器的水位值，如果水位值在5S钟之内连续上升，则报警
 */
object Flink_ProcessFunction_Keyed_Demo {
  def main(args: Array[String]): Unit = {
    // TODO KeyedProcessFunction：会处理流的每一个元素，输出为0个、1个或者多个元素
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //2.定义时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.从端口读取数据，创建DS
    val lineDS: DataStream[String] = envStream.socketTextStream("hadoop202", 9999)

    //4.变换数据结构，拆分数据，并封装为样例类对象
    val waterSensorDS: DataStream[WaterSensor] = lineDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //5.抽取事件时间戳（时间戳单调递增）
    // TODO 默认水位线：watermark = eventTime -1ms
    val timestampsDS: DataStream[WaterSensor] = waterSensorDS.assignAscendingTimestamps(_.ts * 1000)

    //6.按照id进行分流
    val keyByKS: KeyedStream[WaterSensor, String] = timestampsDS.keyBy(_.id)


    // TODO 水位连续5S上涨报警（有延迟999ms）
    //7.遍历每一个元素，执行process
    val processDS: DataStream[String] = keyByKS.process(
      // TODO KeyedProcessFunction[KEY,IN,OUT]
      new KeyedProcessFunction[String, WaterSensor, String] {
        //定义当前水位值数据
        private var currentHeight = 0L
        //定义计时器
        private var alarmTimer = 0L
        //① 每来一条数据，就执行一次该方法
        override def processElement(value: WaterSensor, //输出的参数
                                    ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, //上下文环境
                                    out: Collector[String] //输出的数据集
                                   ) = {
          //a.判断当前水位是否上涨
          if (value.vc > currentHeight) {
            //当前水位大于上一次的水位，则判断是否有定时器
            if (alarmTimer == 0L) {
              //没有定时器，则创建
              alarmTimer = value.ts * 1000 + 5000
              //注册定时器
              ctx.timerService().registerEventTimeTimer(alarmTimer)
            }
          } else {
            //如果当前水位低于或等于上一次的水位，则取消定时器
            ctx.timerService().deleteEventTimeTimer(alarmTimer)
            //更新定时器报警的时间
            alarmTimer = value.ts * 1000 + 5000
            //重新创建定时器
            ctx.timerService().registerEventTimeTimer(alarmTimer)
          }
          //b.更新水位线数据
          currentHeight = value.vc.toLong
        }

        //② 触发定时器的执行
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          //定时器在指定时间执行
          out.collect(s"水位传感器[${ctx.getCurrentKey}],水位线[${ctx.timerService().currentWatermark()}],连续5S水位上涨···")
        }
      }
    )

    //8.打印输出
    processDS.print("process--")

    //9.启动任务
    envStream.execute()
  }
}
