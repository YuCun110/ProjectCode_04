package com.caihua.api.state

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 如果连续两次水位差超过4cm，发生预警信息
 */
object Flink_State_KeyedState {
  def main(args: Array[String]): Unit = {
    // TODO 键控状态：根据输入数据流中定义的键（key）来维护和访问的
    //  数据类型：
    //  1）ValueState[T]保存单个的值
    //  2）ListState[T]保存一个列表
    //  3）MapState[K, V]保存Key-Value对

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
    val markDS: DataStream[WaterSensor] = waterSensorDS.assignTimestampsAndWatermarks(
      new AssignerWithPunctuatedWatermarks[WaterSensor] {
        override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long) = {
          new Watermark(extractedTimestamp)
        }

        override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long) = {
          element.ts * 1000
        }
      }
    )

    //6.按照id进行分组
    val keyByDS: KeyedStream[WaterSensor, String] = markDS.keyBy(_.id)

    // TODO 键控状态：如果连续两次水位差超过4cm，发生预警信息
    //7.预警
    val warnDS: DataStream[String] = keyByDS.process(
      new KeyedProcessFunction[String, WaterSensor, String] {
        // TODO 声明状态类型变量
        //  1.在open方法中完成变量的初始化
        //  2.在变量声明时使用lazy
        //当前水位值
        private var currentHeight: ValueState[Double] = _
        //定时器时间
        private var alarmTimer: ValueState[Long] = _
        //水位差异常次数
        private var exceptionCount: ValueState[Int] = _

        // TODO 使用lazy声明变量
//        private lazy val currentHeight: ValueState[Double] = getRuntimeContext.getState(
//          new ValueStateDescriptor[Double]("currentHeight", classOf[Double])
//        )

        //① 初始化状态变量
        // TODO 数据的状态必须由运行环境指定
        // 构建新的状态时需要指定状态的描述信息以及变量类型
        override def open(parameters: Configuration): Unit = {
          currentHeight = getRuntimeContext.getState(
            new ValueStateDescriptor[Double]("currentHeight", classOf[Double])
          )
          alarmTimer = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
          )
          exceptionCount = getRuntimeContext.getState(
            new ValueStateDescriptor[Int]("exceptionCount", classOf[Int])
          )
        }
        //② 判断水位预警信息
        override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]) = {
          if (value.vc > currentHeight.value() + 4) {
            //水位异常次数加一
            exceptionCount.update(exceptionCount.value() + 1)
          }
          if(exceptionCount.value() >= 2){
            //创建定时器
            alarmTimer.update(value.ts * 1000)
            //注册定时器
            ctx.timerService().registerEventTimeTimer(alarmTimer.value())
            exceptionCount.update(0)
          }
          //更新水位数据
          currentHeight.update(value.vc)
        }
        //③ 相应警报
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect(s"当前观测点[${ctx.getCurrentKey}]连续两次水位差超过4cm")
        }
      }
    )

    //8.打印输出
    keyByDS.print("process--")
    warnDS.print("warning--")

    //9.启动任务
    envStream.execute()
  }

}
