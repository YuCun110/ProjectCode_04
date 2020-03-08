package com.caihua.project.function

import java.lang

import com.caihua.project.bean.HotItemClick
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO WindowFunction<IN, OUT, KEY, W extends Window>
class HotItemWindowFunction extends WindowFunction[Long,HotItemClick,Long,TimeWindow] {
  /**
   * 对统计后的所有商品点击量结果，增加该商品所在的窗口结束时间，并封装为样例类对象
   * @param key    商品ID
   * @param window 窗口信息
   * @param input  统计的各个商品的点击量
   * @param out    各个商品的点击量样例类
   */
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[HotItemClick]): Unit = {
    out.collect(HotItemClick(key,input.iterator.next(),window.getEnd))
  }
}
