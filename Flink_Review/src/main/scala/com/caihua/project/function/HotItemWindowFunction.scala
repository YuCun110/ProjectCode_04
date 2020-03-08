package com.caihua.project.function

import com.caihua.project.bean.HotItemInfo
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO WindowFunction[IN, OUT, KEY, W <: Window]
class HotItemWindowFunction extends WindowFunction[Long,HotItemInfo,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[HotItemInfo]): Unit = {
    out.collect(HotItemInfo(key,input.iterator.next(),window.getEnd))
  }
}
