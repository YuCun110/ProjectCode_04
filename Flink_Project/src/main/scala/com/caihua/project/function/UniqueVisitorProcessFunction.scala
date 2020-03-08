package com.caihua.project.function

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO ProcessAllWindowFunction[IN, OUT, W <: Window]
class UniqueVisitorProcessFunction extends ProcessAllWindowFunction[(Long,Int),String,TimeWindow]{
  override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {
    //1.创建有去重效果的集合Set
    val userIdContainer = mutable.Set[Long]()

    //2.将所有窗口中的数据进行遍历，存入Set集合
    for (elem <- elements) {
      userIdContainer.add(elem._1)
    }

    //3.计算去重后的用户数，将结果写入字符串中
    val result = new mutable.StringBuilder()
    result.append(s"当前时间：${context.window.getEnd}\n")
    result.append(s"独立访客数量：${userIdContainer.size}\n")
    result.append("========================")

    //4.将结果返回
    out.collect(result.toString())
  }
}
