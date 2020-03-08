package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.bean.ServiceLog
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.HotResourcesAnalysesDao
import com.caihua.project.function.{HotResourcesAggregateFunction, HotResourcesProcessFunction, HotResourcesWindowFunction, MyAggregateFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 */
class HotResourcesAnalysesService extends TService{
  /**
   * 获取持久层对象
   * @return
   */
  override def getDao(): TDao = new HotResourcesAnalysesDao

  /**
   * 统计热门资源的点击量排行
   */
  override def analyses() = {
    //1.获取数据
    val serviceLogDS: DataStream[bean.ServiceLog] = getServiceLog()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.ServiceLog] = serviceLogDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[bean.ServiceLog](Time.minutes(1)) {
        override def extractTimestamp(element: bean.ServiceLog) = {
          element.eventTime
        }
      }
    )

    //3.按照url分组
    val keyByUrlKS: KeyedStream[bean.ServiceLog, String] = markDS.keyBy(_.url)

    //4.开窗：10分钟滚动，5秒钟滑动
    val windowWS: WindowedStream[bean.ServiceLog, String, TimeWindow] = keyByUrlKS.timeWindow(Time.minutes(10),Time.seconds(5))

    //5.按照key进行聚合，统计不同资源的点击量，并对聚合后的结果进行封装
//    val aggregateDS: DataStream[bean.HotResourceClick] = windowWS.aggregate(
//      new HotResourcesAggregateFunction,
//      new HotResourcesWindowFunction
//    )
    val aggregateDS: DataStream[bean.HotResourceClick] = windowWS.aggregate(
      new MyAggregateFunction[ServiceLog],
      new HotResourcesWindowFunction
    )

    //6.按照窗口（各个窗口的结尾时间）进行分组
    val groupByWindowKS: KeyedStream[bean.HotResourceClick, Long] = aggregateDS.keyBy(_.WindowEndTime)

    //7.对所有窗口中的元素，按照点击量进行排序
    val resultDS: DataStream[String] = groupByWindowKS.process(
      new HotResourcesProcessFunction
    )

    //8.返回结果
    resultDS
  }
}
