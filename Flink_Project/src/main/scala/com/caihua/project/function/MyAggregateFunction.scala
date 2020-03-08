package com.caihua.project.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO AggregateFunction<IN, ACC, OUT>
class MyAggregateFunction[T] extends AggregateFunction[T,Long,Long]{
  //1.初始化累计值
  override def createAccumulator(): Long = 0L

  //2.分区内求和
  override def add(in: T, acc: Long): Long = acc + 1L

  //3.返回累加的最终结果
  override def getResult(acc: Long): Long = acc

  //4.分区间求和
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
