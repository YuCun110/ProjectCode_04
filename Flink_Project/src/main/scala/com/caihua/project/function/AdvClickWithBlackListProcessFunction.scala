package com.caihua.project.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class AdvClickWithBlackListProcessFunction extends KeyedProcessFunction[(String, Long), ((String, Long), Long), ((String, Long), Long)] {
  //定义键控状态变量
  //1.定义广告点击量累加器
  private var adClickCount: ValueState[Long] = _

  //2.定义标记
  private var flag: ValueState[Boolean] = _

  //3.初始化状态变量
  override def open(parameters: Configuration): Unit = {
    adClickCount = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("adClickCount", classOf[Long])
    )
    flag = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("flag", classOf[Boolean])
    )
  }

  //4.遍历元素，对广告点击量进行累加
  override def processElement(value: ((String, Long), Long), ctx: KeyedProcessFunction[(String, Long), ((String, Long), Long), ((String, Long), Long)]#Context, out: Collector[((String, Long), Long)]): Unit = {
    //① 获取广告的点击量
    var currentCount: Long = adClickCount.value()

    //① 获取第一条数据的处理时间，设置一天的有效期
    if (currentCount == 0) {
      //获取数据的处理时间
      val currentTime: Long = ctx.timerService().currentProcessingTime()
      //注册定时器，一天后执行
      val currentDay: Long = currentTime / (24 * 60 * 60 * 1000)
      val nextDaytimestamp: Long = (currentDay + 1) * 24 * 60 * 60 * 1000
      ctx.timerService().registerProcessingTimeTimer(nextDaytimestamp)
    }
    //② 对当前广告的点击量加一
    currentCount = currentCount + 1
    //③ 判断当前点击量是否超过阈值
    if (currentCount >= 100 && !flag.value()) {
      //点击量超过阈值，检查该广告是否被标记过
      if (!flag.value()) {
        //该广告点击量超过阈值，并且没有标记过，则输出到侧输出流中
        val output = new OutputTag[((String, Long), Long)]("BlackList")
        ctx.output(output, value)
      }
      //该广告点击量超过阈值，并且已经被标记过，则不予处理
    } else {
      //点击量没有超过阈值，正常输出
      out.collect(value)
    }
    //④ 更新点击量
    adClickCount.update(currentCount)
  }

  //5.启动定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, Long), ((String, Long), Long), ((String, Long), Long)]#OnTimerContext, out: Collector[((String, Long), Long)]): Unit = {
    //清空状态变量
    adClickCount.clear()
    flag.clear()
  }
}
