package com.caihua.project.function

import java.util

import com.caihua.project.bean.HotItemClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO ProcessFunction<I, O>
class HotItemProcessFunction extends KeyedProcessFunction[Long,HotItemClick,String] {
  //定义键控状态变量
  //1.定义容器，存储当前窗口的所有数据
  private var itemList: ListState[HotItemClick] = _
  //2.定义定时器
  private var alarmTimer: ValueState[Long] = _

  //3.状态变量初始化
  override def open(parameters: Configuration): Unit = {
    itemList = getRuntimeContext.getListState(
      new ListStateDescriptor[HotItemClick]("itemList",classOf[HotItemClick])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTimer",classOf[Long])
    )
  }

  //4.将每个数据放入容器中
  override def processElement(value: HotItemClick, ctx: KeyedProcessFunction[Long, HotItemClick, String]#Context, out: Collector[String]): Unit = {
    //① 存放数据
    itemList.add(value)
    //② 设置定时器
    if(alarmTimer.value() == 0){
      //注册定时器
      ctx.timerService().registerEventTimeTimer(value.windowEnd)
      //定时时间
      alarmTimer.update(value.windowEnd)
    }
  }

  //5.触发定时器，按照商品的点击量倒序排序
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemClick, String]#OnTimerContext, out: Collector[String]): Unit = {
    //① 获取当前窗口中的所有商品的点击量数据迭代器
    val itemIter: util.Iterator[HotItemClick] = itemList.get().iterator()
    //② 定义排序集合
    val dataList = new ListBuffer[HotItemClick]
    //③ 遍历数据，放入排序集合
    while (itemIter.hasNext){
      dataList.append(itemIter.next())
    }
    //④ 清除状态变量中的数据
    itemList.clear()
    alarmTimer.clear()
    //⑤ 排序
    //val sortItems: ListBuffer[HotItemClick] = dataList.sortWith(_.clickCount > _.clickCount).take()
    val sortItems: ListBuffer[HotItemClick] = dataList.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)
    //⑥ 将结果抓换为字符串，输出
    val result = new StringBuilder
    result.append(s"当前时间：$timestamp\n")
    for (elem <- sortItems) {
      result.append(s"商品ID：${elem.itemId}，点击数量：${elem.clickCount}\n")
    }
    result.append("=========================")
    //⑦ 输出
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
