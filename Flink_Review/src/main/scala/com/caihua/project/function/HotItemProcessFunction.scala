package com.caihua.project.function

import java.util

import com.caihua.project.bean.HotItemInfo
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO KeyedProcessFunction<K, I, O>
class HotItemProcessFunction extends KeyedProcessFunction[Long,HotItemInfo,String]{
  //定义键控状态变量
  //1.定义容器
  private var itemList: ListState[HotItemInfo] = _
  //2.定义定时器
  private var alarmTime: ValueState[Long] = _

  //3.初始化状态变量
  override def open(parameters: Configuration): Unit = {
    itemList = getRuntimeContext.getListState(
      new ListStateDescriptor[HotItemInfo]("itemList",classOf[HotItemInfo])
    )
    alarmTime = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTime",classOf[Long])
    )
  }

  //4.遍历窗口中的所有元素，放入容器中
  override def processElement(value: HotItemInfo, ctx: KeyedProcessFunction[Long, HotItemInfo, String]#Context, out: Collector[String]): Unit = {
    //① 将元素放入容器
    itemList.add(value)
    //② 设置定时器
    if(alarmTime.value() == 0L){
      alarmTime.update(value.windowEndTime)
      ctx.timerService().registerEventTimeTimer(alarmTime.value())
    }
  }

  //5.启动定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemInfo, String]#OnTimerContext, out: Collector[String]): Unit = {
    //① 获取容器中的数据
    val items: util.Iterator[HotItemInfo] = itemList.get().iterator()
    //② 按照点击量进行排序，取Top3
    //导入隐式转换
    import scala.collection.JavaConversions._
    val top3: List[HotItemInfo] = items.toList.sortWith(_.clickCount > _.clickCount).take(3)
    //③ 清除状态变量
    itemList.clear()
    alarmTime.clear()
    //④ 将结果转换为字符串
    val result = new StringBuilder
    result.append(s"当前时间：$timestamp\n")
    for (elem <- top3) {
      result.append(s"商品id：${elem.itemId}，点击数量：${elem.clickCount}\n")
    }
    result.append("===================")
    //⑤ 返回结果
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
