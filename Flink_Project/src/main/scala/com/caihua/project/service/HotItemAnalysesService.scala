package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.bean.UserBehavior
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.HotItemAnalysesDao
import com.caihua.project.function.{HotItemAggregateFunction, HotItemProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 热门商品的分析
 */
class HotItemAnalysesService extends TService{

  /**
   * 获取持久层对象
   * @return
   */
  override def getDao(): TDao = new HotItemAnalysesDao

  /**
   * 每隔5分钟输出最近一小时内点击量最多的前N个商品
   */
  override def analyses()={
    //1.获取用户行为数据
    val userBehaviorDS: DataStream[UserBehavior] = getUserBehavir()

    //2.抽取事件时间戳，设置水位线(默认：EventTime - 1ms)
    val markDS: DataStream[UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000)

    //3.ETL：筛选商品点击量相关的数据
    val filterDS: DataStream[UserBehavior] = markDS.filter(_.behavior == "pv")

    //4.按照商品ID对数据进行分组
    val keyByKS: KeyedStream[UserBehavior, Long] = filterDS.keyBy(_.itemId)

    //5.开启窗口：1小时滚动，5分钟滑动
    val windowWS: WindowedStream[UserBehavior, Long, TimeWindow] = keyByKS.timeWindow(Time.hours(1),Time.minutes(5))

    //6.在窗口中按照key聚合，统计各个商品的点击量，并对聚合结果进行变换，方便排序
    // TODO Ⅰ.窗口中单一数据处理：aggregate；全量数据处理：process
    // TODO Ⅱ.aggregate：第一个参数表示聚合函数，第二个参数表示当前窗口的处理函数
    //  第一个函数的处理结果会作为第二个函数的输入值
    //当窗口数据聚合后，会将所有窗口的数据全部打乱，其结果是所有数据
    val itemClickDS: DataStream[bean.HotItemClick] = windowWS.aggregate(
      new HotItemAggregateFunction, //按照相同Key对点击量进行累加
      new HotItemWindowFunction //对统计后的所有商品点击量结果，增加该商品所在的窗口结束时间，并封装为样例类对象
    )

    //7.将统计的结果，按照窗口的结束时间，再次将不同的窗口分组
    val keyByWindowKS: KeyedStream[bean.HotItemClick, Long] = itemClickDS.keyBy(_.windowEnd)

    // TODO Ⅲ.process：将窗口的全量数据处理
    //8.按照各个窗口的不同商品的点击量进行排序（倒序）
    val resultDS: DataStream[String] = keyByWindowKS.process(
      new HotItemProcessFunction
    )

    //9.返回结果
    resultDS
  }
}
