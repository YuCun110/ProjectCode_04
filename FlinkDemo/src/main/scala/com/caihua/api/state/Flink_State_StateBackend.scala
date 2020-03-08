package com.caihua.api.state

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_State_StateBackend {
  def main(args: Array[String]): Unit = {
    // TODO Ⅰ.状态后端：主要负责两件事
    //  1.本地的状态管理
    //  2.将检查点（checkpoint）状态写入远程存储
    // TODO Ⅱ.分类
    //  1.MemoryStateBackend
    //  2.FsStateBackend
    //  3.RocksDBStateBackend

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //2.定义时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // TODO 设定状态后端
    // 默认后端为内存
    val backend = new RocksDBStateBackend("hdfs:hadoop202:9000/checkpoint")
    envStream.setStateBackend(backend)
    // TODO 启用检查点
    // 这里的检查点其实和watermark是一样的，表示一种状态数据
    // 参数中的1000表示1s生成一条checkpoint标记数据
    // 参数中EXACTLY_ONCE表示精准一次性处理
    envStream.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)

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

    //7.预警
    val warnDS: DataStream[String] = keyByDS.process(
      new KeyedProcessFunction[String, WaterSensor, String] {
        // TODO 声明状态类型变量
        private var currentHeight: ValueState[Double] = _
        private var alarmTimer: ValueState[Long] = _
        private var exceptionCount: ValueState[Int] = _
        //① 初始化状态变量
        // TODO 数据的状态必须由运行环境指定
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
