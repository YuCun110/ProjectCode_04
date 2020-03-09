package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.bean.{OrderEvent, ReceiptEvent}
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.OrderTransactionAnalysesDao
import com.caihua.project.function.OrderTransactionProcessFunction
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class OrderTransactionAnalysesService extends TService {
  /**
   * 获取持久层对象
   *
   * @return
   */
  override def getDao(): TDao = new OrderTransactionAnalysesDao

  /**
   * 统计分析
   *
   * @return
   */
  override def analyses() = {
    // TODO 双流匹配
    //1.获取两个数据的数据流
    val orderDS: DataStream[bean.OrderEvent] = getOrderEvent()
    val receiptDS: DataStream[bean.ReceiptEvent] = getReceiptEvent()

    //2.抽取事件时间戳
    val orderMarkDS: DataStream[bean.OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)
    val receiptMarkDS: DataStream[bean.ReceiptEvent] = receiptDS.assignAscendingTimestamps(_.eventTime * 1000L)

    //3.按照orderID分组
    val orderKS: KeyedStream[bean.OrderEvent, String] = orderMarkDS.keyBy(_.txId)
    val receiptKS: KeyedStream[bean.ReceiptEvent, String] = receiptMarkDS.keyBy(_.txId)

    //4.连接两个流
    val resultDS: DataStream[String] = orderKS.connect(receiptKS)
      .process(
        new OrderTransactionProcessFunction
      )

    //5.输出统计结果
    resultDS
  }

  def analysesByJoin() = {
    // TODO 双流JOIN
    //1.获取两个数据的数据流
    val orderDS: DataStream[bean.OrderEvent] = getOrderEvent()
    val receiptDS: DataStream[bean.ReceiptEvent] = getReceiptEvent()

    //2.抽取事件时间戳
    val orderMarkDS: DataStream[bean.OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)
    val receiptMarkDS: DataStream[bean.ReceiptEvent] = receiptDS.assignAscendingTimestamps(_.eventTime * 1000L)

    //3.按照orderID分组
    val orderKS: KeyedStream[bean.OrderEvent, String] = orderMarkDS.keyBy(_.txId)
    val receiptKS: KeyedStream[bean.ReceiptEvent, String] = receiptMarkDS.keyBy(_.txId)

    //4.join
    val resultDS: DataStream[String] = orderKS.intervalJoin(receiptKS)
      .between(Time.minutes(-5), Time.minutes(5))
      .process(
        new ProcessJoinFunction[OrderEvent, ReceiptEvent, String] {
          override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, String]#Context, out: Collector[String]) =
            out.collect(s"交易成功：交易ID：${left.txId}，对账完成")
        }
      )

    //5.输出统计结果
    resultDS
  }
}
