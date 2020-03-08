package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.bean.UserBehavior
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.HotItemAnalysesDao
import com.caihua.project.function.{HotItemProcessFunction, HotItemWindowFunction, MyAggregateFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author XiLinShiShan
 * @version 0.0.1
 *          每隔5分钟统计一次近一个小时每个商品的点击量
 */
class HotItemAnalysesService extends TService{
  /**
   * 获取持久层对象
   *
   * @return
   */
  override def getDao: TDao = new HotItemAnalysesDao

  /**
   * 统计分析
   *
   * @return
   */
  override def analyses() = {
    //1.获取原始数据
    val dataDa: DataStream[bean.UserBehavior] = getDao.getUserBehavior()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.UserBehavior] = dataDa.assignAscendingTimestamps(_.timestamp * 1000)

    //3.过滤出点击事件
    val filterDS: DataStream[bean.UserBehavior] = markDS.filter(_.behavior == "pv")

    //4.分组：按照商品ID进行分组
    val groupByItemKS: KeyedStream[bean.UserBehavior, Long] = filterDS.keyBy(_.itemID)

    //5.开窗：一个小时滚动窗口，5分钟滑动窗口
    val windowWS: WindowedStream[bean.UserBehavior, Long, TimeWindow] = groupByItemKS.timeWindow(Time.hours(1),Time.minutes(5))

    //6.对开窗后的数据，按照商品ID进行聚合，统计各个商品的点击量，并且将结果封装为样例类对象
    val aggregateDS: DataStream[bean.HotItemInfo] = windowWS.aggregate(
      new MyAggregateFunction[UserBehavior],
      new HotItemWindowFunction
    )

    //7.将所有开窗的数据，按照开窗的结束时间分组
    val groupByWindow: KeyedStream[bean.HotItemInfo, Long] = aggregateDS.keyBy(_.windowEndTime)

    //8.根据各个窗口的各个商品统计点击量排序，取TopN
    val resultDS: DataStream[String] = groupByWindow.process(
      new HotItemProcessFunction
    )

    //9.返回结果
    resultDS
  }
}
