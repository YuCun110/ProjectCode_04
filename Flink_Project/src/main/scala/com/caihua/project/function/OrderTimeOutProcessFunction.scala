package com.caihua.project.function

import com.caihua.project.bean.{OrderEvent, OrderMergePay}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class OrderTimeOutProcessFunction extends KeyedProcessFunction[Long, OrderEvent, String] {
  //定义键控状态变量
  //1.定义历史数据
  private var lastOrder: ValueState[OrderMergePay] = _
  //2.定义定时器
  private var alarmTimer: ValueState[Long] = _

  //3.初始化
  override def open(parameters: Configuration): Unit = {
    lastOrder = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderMergePay]("lastOrder", classOf[OrderMergePay])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
    )
  }

  //4.遍历元素，获取订单数据，判断支付超时
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]): Unit = {
    //① 获取上一个元素
    val lastOrderMerge: OrderMergePay = lastOrder.value()
    //② 创建侧输出流
    val outputTag = new OutputTag[String]("TimeOut")
    //③ 判断当前数据是否为下单数据还是支付数据
    if (value.eventType == "create") {
      //当前数据为下单信息
      if (lastOrderMerge == null) {
        //该元素为下单数据，并是第一个元素
        //a.将该元素的状态保存
        lastOrder.update(OrderMergePay(value.orderId, value.eventTime, 0L))
        //b.创建定时器，如果十五分钟后没有支付，则触发执行
        alarmTimer.update((value.eventTime + 15 * 60) * 1000)
        ctx.timerService().registerEventTimeTimer(alarmTimer.value())
      } else {
        //该元素为下单数据，不是第一个元素（上一个元素为支付数据）
        //a.计算数据差值
        val diff: Long = Math.abs(lastOrderMerge.payTS - value.eventTime)
        //b.判断超时状态
        if (diff >= 15 * 60) {
          //支付已经超时，将信息输出至侧输出流
          ctx.output(outputTag, s"超时订单--订单ID：${value.orderId}，耗时：[$diff]秒")
        } else {
          //支付未超时，正常输出
          out.collect(s"交易正常--订单ID：${value.orderId}，耗时：[$diff]秒")
        }
        //c.所有数据均已到达，删除定时器和状态变量
        ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
        alarmTimer.clear()
        lastOrder.clear()
      }
    } else {
      //当前数据为支付信息
      if (lastOrderMerge == null) {
        //支付信息，并且第一个到达，等待create的到来
        lastOrder.update(OrderMergePay(value.orderId, 0L, value.eventTime))
        //设置定时器，等待5分钟
        alarmTimer.update((value.eventTime + 5 * 60) * 1000)
        ctx.timerService().registerEventTimeTimer(alarmTimer.value())
      } else {
        //下单数据已经到达，核算支付是否超时
        //a.计算数据差值
        val diff: Long = Math.abs(lastOrderMerge.orderTS - value.eventTime)
        //b.判断超时状态
        if (diff >= 15 * 60) {
          //支付已经超时，将信息输出至侧输出流
          ctx.output(outputTag, s"超时订单--订单ID：${value.orderId}，耗时：[$diff]秒")
        } else {
          //支付未超时，正常输出
          out.collect(s"交易正常--订单ID：${value.orderId}，耗时：[$diff]秒")
        }
        //c.所有数据均已到达，删除定时器和状态变量
        ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
        alarmTimer.clear()
        lastOrder.clear()
      }
    }
  }

  //5.启动定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
    //如果定时器触发，说明有数据未到达
    //① 获取状态变量
    val lastOrderMerge: OrderMergePay = lastOrder.value()
    //② 定义侧输出流
    val output = new OutputTag[String]("TimeOut")
    //③ 判断
    if (lastOrderMerge.orderTS != 0) {
      ctx.output(output, s"缺失订单--订单ID：${lastOrderMerge.orderId}，没有收到支付数据，交易失败")
    } else {
      ctx.output(output,s"缺失订单--订单ID：${lastOrderMerge.orderId}，没有收到下单数据，交易有问题")
    }
    //④ 清空状态变量
    alarmTimer.clear()
    lastOrder.clear()
  }
}
