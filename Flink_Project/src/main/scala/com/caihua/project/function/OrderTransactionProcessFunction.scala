package com.caihua.project.function

import com.caihua.project.bean.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class OrderTransactionProcessFunction extends CoProcessFunction[OrderEvent, ReceiptEvent, String] {
  //定义两个容器
  private var orderMap: MapState[String, String] = _
  private var receiptMap: MapState[String, String] = _

  //初始化
  override def open(parameters: Configuration): Unit = {
    orderMap = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, String]("orderMap", classOf[String], classOf[String])
    )
    receiptMap = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, String]("receiptMap", classOf[String], classOf[String])
    )
  }

  //用order中的支付ID匹配receipt容器中的支付ID
  override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, String]#Context, out: Collector[String]): Unit = {
    //① 获取容器，进行匹配
    val txId: String = receiptMap.get(value.txId)
    //② 判断
    if (txId == null) {
      orderMap.put(value.txId, "tx")
    } else {
      out.collect(s"交易成功：交易ID：${value.txId}，对账完成")
      orderMap.remove(value.txId)
    }
  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, String]#Context, out: Collector[String]): Unit = {
    //① 获取容器，进行匹配
    val txId: String = orderMap.get(value.txId)
    //② 判断
    if (txId == null) {
      receiptMap.put(value.txId, "tx")
    } else {
      out.collect(s"交易成功：交易ID：${value.txId}，对账完成")
      receiptMap.remove(value.txId)
    }
  }
}
