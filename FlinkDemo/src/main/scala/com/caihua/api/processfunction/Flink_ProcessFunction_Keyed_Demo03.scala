package com.caihua.api.processfunction

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 *          监控水位传感器的水位值，如果水位值在5S钟之内连续上升，则报警
 */
object Flink_ProcessFunction_Keyed_Demo03 {
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

    //5.抽取事件时间戳，自定义水位线
    // TODO 自定义水位线：AssignerWithPunctuatedWatermarks
    val timestampsDS: DataStream[WaterSensor] = waterSensorDS.assignTimestampsAndWatermarks(
      //间歇式的生成水位线
      new AssignerWithPunctuatedWatermarks[WaterSensor] {
        //② 定义水位线
        override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long) = {
          //将水位线设置为当前事件的时间戳
          new Watermark(extractedTimestamp)
        }
        //① 抽取事件的时间戳
        override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long) = {
          element.ts * 1000
        }
      }
    )

    //6.按照id进行分流
    val keyByKS: KeyedStream[WaterSensor, String] = timestampsDS.keyBy(_.id)


    // TODO 水位连续5S上涨报警，并将异常水位值放入侧输出流（异常水位：高于上一次水位值5cm）
    //7.遍历每一个元素，执行process
    val processDS: DataStream[String] = keyByKS.process(
      // TODO KeyedProcessFunction[KEY,IN,OUT]
      new KeyedProcessFunction[String, WaterSensor, String] {
        //定义当前水位值数据
        private var currentHeight = 0L
        //定义计时器
        private var alarmTimer = 0L
        //定义侧输出流
        val outputTag = new OutputTag[Double]("ExceptWaterSensor")
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

          //b.判断异常水位值
          if(value.vc >= currentHeight + 5){
            //水位异常，则输出到侧输出流中
            ctx.output(outputTag,value.vc)
          }else{
            //水位正常，更新水位线数据
            currentHeight = value.vc.toLong
          }
        }

        //② 触发定时器的执行
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          //定时器在指定时间执行
          out.collect(s"水位传感器[${ctx.getCurrentKey}],水位线[${ctx.timerService().currentWatermark()}],连续5S水位上涨···")
        }
      }
    )

    //处理侧输出流
    val outputTagDS: DataStream[Double] = processDS.getSideOutput(new OutputTag[Double]("ExceptWaterSensor"))

    //8.打印输出
    keyByKS.print("water--")
    processDS.print("process--")
    outputTagDS.print("outputTag--")

    //9.启动任务
    envStream.execute()
  }
}
