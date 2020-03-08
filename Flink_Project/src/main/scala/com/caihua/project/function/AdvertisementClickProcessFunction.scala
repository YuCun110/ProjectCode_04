package com.caihua.project.function

import java.util

import com.caihua.project.bean.CountByProvince
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class AdvertisementClickProcessFunction extends KeyedProcessFunction[Long,CountByProvince,String]{
  //定义键控状态变量
  //1.定义容器
  private var container: ListState[CountByProvince] = _

  //2.定义定时器
  private var alarmTimer: ValueState[Long] = _

  //3.初始化状态变量
  override def open(parameters: Configuration): Unit = {
    container = getRuntimeContext.getListState(
      new ListStateDescriptor[CountByProvince]("container",classOf[CountByProvince])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTimer",classOf[Long])
    )
  }

  //4.遍历窗口中的所有数据，并存放在容器中
  override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[Long, CountByProvince, String]#Context, out: Collector[String]): Unit = {
    //① 将元素放入容器中
    container.add(value)
    //② 设置定时器
    if(alarmTimer.value() == 0L){
      alarmTimer.update(value.windowEnd)
      ctx.timerService().registerEventTimeTimer(alarmTimer.value())
    }
  }

  //5.启动定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, CountByProvince, String]#OnTimerContext, out: Collector[String]): Unit = {
    //① 获取容器中的数据
    val datas: util.Iterator[CountByProvince] = container.get().iterator()
    //② 对数据进行排序
    //导入隐式转换
    import scala.collection.JavaConversions._
    val sortList: List[CountByProvince] = datas.toList.sortWith(_.count > _.count)
    //③ 清空状态变量信息
    container.clear()
    alarmTimer.clear()
    //④ 定义字符串，将结果封装输出
    val result = new StringBuilder
    result.append(s"当前时间：$timestamp，广告点击详情：\n")
    for (elem <- sortList) {
      result.append(s"所在省份：${elem.province}，广告ID：${elem.adid}，点击量：${elem.count}\n")
    }
    result.append("==================================")

    //Thread.sleep(500)

    //⑤ 输出
    out.collect(result.toString())
  }
}
