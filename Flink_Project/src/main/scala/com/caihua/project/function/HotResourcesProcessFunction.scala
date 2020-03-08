package com.caihua.project.function

import java.util

import com.caihua.project.bean.HotResourceClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO KeyedProcessFunction<K, I, O>
class HotResourcesProcessFunction extends KeyedProcessFunction[Long, HotResourceClick, String] {
  //定义键控状态变量
  //1.定义容器
  private var resourceList: ListState[HotResourceClick] = _
  //2.定义定时器
  private var alarmTimer: ValueState[Long] = _

  //3.初始化状态变量
  override def open(parameters: Configuration): Unit = {
    resourceList = getRuntimeContext.getListState(
      new ListStateDescriptor[HotResourceClick]("resourceList", classOf[HotResourceClick])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
    )
  }

  //4.遍历每一个元素，并将器放入容器中
  override def processElement(value: HotResourceClick, ctx: KeyedProcessFunction[Long, HotResourceClick, String]#Context, out: Collector[String]): Unit = {
    //将元素放入容器中
    resourceList.add(value)
    //设置定时器
    if(alarmTimer.value() == 0L){
      alarmTimer.update(value.WindowEndTime)
      ctx.timerService().registerEventTimeTimer(alarmTimer.value())
    }
  }

  //5.启动定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotResourceClick, String]#OnTimerContext, out: Collector[String]): Unit = {
    //① 获取所有窗口中的所有元素
    val listIter: util.Iterator[HotResourceClick] = resourceList.get().iterator()
    //② 按照点击量进行倒序排序
    //导入隐式转换，将Java中的集合转换为Scala中的集合
    import scala.collection.JavaConversions._
    val clickCount: List[HotResourceClick] = listIter.toList.sortWith(_.clickCount > _.clickCount).take(3)
    //③ 清除状态变量的信息
    resourceList.clear()
    alarmTimer.clear()
    //④ 将结果转换为字符串，输出
    val result = new StringBuilder
    result.append(s"当前时间：$timestamp\n")
    for (elem <- clickCount) {
      result.append(s"资源地址：${elem.url}，点击数量：${elem.clickCount}\n")
    }
    result.append("==================")
    //⑤ 输出结果
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
