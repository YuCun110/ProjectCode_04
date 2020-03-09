package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.bean.OrderEvent
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.OrderTimeOutAnalysesDao
import com.caihua.project.function.OrderTimeOutProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class OrderTimeOutAnalysesService extends TService{
  /**
   * 获取持久层对象
   *
   * @return
   */
  override def getDao(): TDao = new OrderTimeOutAnalysesDao

  /**
   * 统计分析
   *
   * @return
   */
  override def analyses() = {
    //1.获取原始数据
    val dataDS: DataStream[bean.OrderEvent] = getOrderEvent()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.OrderEvent] = dataDS.assignAscendingTimestamps(_.eventTime * 1000)

    //3.按照订单id进行分组，统计支付超时的数据
    val timeOutDS: DataStream[String] = markDS.keyBy(_.orderId)
      .process(
        new OrderTimeOutProcessFunction
      )

    //4.获取侧输出流
    val outputTag = new OutputTag[String]("TimeOut")
    timeOutDS.getSideOutput(outputTag).print("warn--")

    //5.正常输出
    timeOutDS
  }

  def analysesByCEP() = {
    //1.获取原始数据
    val dataDS: DataStream[bean.OrderEvent] = getOrderEvent()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.OrderEvent] = dataDS.assignAscendingTimestamps(_.eventTime * 1000)

    //3.按照订单id进行分组
    val orderIdKS: KeyedStream[bean.OrderEvent, Long] = markDS.keyBy(_.orderId)

    // TODO 用CEP判断用户支付是否超时(15分钟)
    //4.定义规则
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    //5.应用规则
    val timeOutPS: PatternStream[OrderEvent] = CEP.pattern(orderIdKS,pattern)

    //6.获取结果
    val resultDS: DataStream[String] = timeOutPS.select(map => {
      //获取订单信息
      val create: OrderEvent = map("begin").iterator.next()
      val pay: OrderEvent = map("follow").iterator.next()
      s"订单ID：${create.orderId}，支付延迟：${pay.eventTime - create.eventTime}秒"
    })

    //7.返回统计结果
    resultDS
  }
}
