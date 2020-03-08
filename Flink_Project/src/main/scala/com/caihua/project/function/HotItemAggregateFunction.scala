package com.caihua.project.function

import com.caihua.project.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 按照相同Key对点击量进行累加
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO 聚合函数：AggregateFunction<IN, ACC, OUT>
class HotItemAggregateFunction extends AggregateFunction[UserBehavior,Long,Long]{
  //累加器初始化
  override def createAccumulator(): Long = 0L

  //分区内累加
  override def add(in: UserBehavior, acc: Long): Long = {acc + 1L}

  //返回累加结果
  override def getResult(acc: Long): Long = acc

  //分区间累加
  override def merge(acc: Long, acc1: Long): Long = {acc + acc1}
}
