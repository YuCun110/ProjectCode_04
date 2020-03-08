package com.caihua.project.function

import java.lang

import com.caihua.project.bean.HotResourceClick
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO WindowFunction<IN, OUT, KEY, W extends Window>
class HotResourcesWindowFunction extends WindowFunction[Long,HotResourceClick,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[HotResourceClick]): Unit = {
    out.collect(HotResourceClick(key,input.iterator.next(),window.getEnd))
  }
}
