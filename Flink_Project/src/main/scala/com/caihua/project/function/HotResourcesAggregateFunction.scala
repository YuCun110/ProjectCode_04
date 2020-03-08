package com.caihua.project.function

import com.caihua.project.bean.ServiceLog
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO AggregateFunction<IN, ACC, OUT>
class HotResourcesAggregateFunction extends AggregateFunction[ServiceLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ServiceLog, acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
