package com.caihua.project.function

import com.caihua.project.bean.CountByProvince
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
  // TODO WindowFunction[IN, OUT, KEY, W <: Window]
class AdvertisementClickWindowFunction extends WindowFunction[Long,CountByProvince,(String,Long),TimeWindow]{
  override def apply(key: (String, Long), window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(key._2,key._1,input.iterator.next(),window.getEnd))
  }
}
