package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.AdvertisementClickAnalysesDao
import com.caihua.project.function.{AdvClickWithBlackListProcessFunction, AdvertisementClickProcessFunction, AdvertisementClickWindowFunction, MyAggregateFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class AdvertisementClickAnalysesService extends TService{
  /**
   * 获取持久层对象
   *
   * @return
   */
  override def getDao(): TDao = new  AdvertisementClickAnalysesDao

  /**
   * 统计分析
   *
   * @return
   */
  override def analyses() = {
    //1.获取原始数据
    val dataDS: DataStream[bean.AdClickLog] = getAdClickLog()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.AdClickLog] = dataDS.assignAscendingTimestamps(_.timestamp * 1000)

    //3.转换数据结构，并按照省份和广告ID进行分组
    val groupByKs: KeyedStream[((String, Long), Long), (String, Long)] = markDS.map(ad => ((ad.province, ad.adId), 1L))
      .keyBy(_._1)

    //4.开窗：一个小时滚动，5秒滑动
    val windowWs: WindowedStream[((String, Long), Long), (String, Long), TimeWindow] = groupByKs.timeWindow(Time.hours(1),Time.seconds(5))

    //5.聚合：按照省份和广告统计广告的点击量，并将结果封装为样例类对象
    val aggregateDS: DataStream[bean.CountByProvince] = windowWs.aggregate(
      new MyAggregateFunction[((String, Long), Long)],
      new AdvertisementClickWindowFunction
    )

    //6.按照各个窗口对数据进行分组，并按照广告的点击量进行排序
    val resultDS: DataStream[String] = aggregateDS.keyBy(_.windowEnd)
      .process(new AdvertisementClickProcessFunction)

    //7.输出统计结果
    resultDS
  }

  /**
   * 统计分析：带有黑名单过滤（一天内点击超过100次广告属于恶意点击）
   *
   * @return
   */
  def analysesWithBlackList() = {
    //1.获取原始数据
    val dataDS: DataStream[bean.AdClickLog] = getAdClickLog()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.AdClickLog] = dataDS.assignAscendingTimestamps(_.timestamp * 1000)

    //3.转换数据结构，并按照省份和广告ID进行分组
    val groupByKs: KeyedStream[((String, Long), Long), (String, Long)] = markDS.map(ad => ((ad.province, ad.adId), 1L))
      .keyBy(_._1)

    // TODO 处理异常数据：黑名单
    val checkDS: DataStream[((String, Long), Long)] = groupByKs.process(
      new AdvClickWithBlackListProcessFunction
    )

    //获取黑名单中的数据，并打印输出
    val output = new OutputTag[((String, Long), Long)]("BlackList")
    val blackListDS: DataStream[((String, Long), Long)] = checkDS.getSideOutput(output)
    blackListDS.print("BlackLists--")

    //4.开窗：一个小时滚动，5秒滑动
    val windowWs: WindowedStream[((String, Long), Long), (String, Long), TimeWindow] = checkDS
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(5))

    //5.聚合：按照省份和广告统计广告的点击量，并将结果封装为样例类对象
    val aggregateDS: DataStream[bean.CountByProvince] = windowWs.aggregate(
      new MyAggregateFunction[((String, Long), Long)],
      new AdvertisementClickWindowFunction
    )

    //6.按照各个窗口对数据进行分组，并按照广告的点击量进行排序
    val resultDS: DataStream[String] = aggregateDS.keyBy(_.windowEnd)
      .process(new AdvertisementClickProcessFunction)

    //7.输出统计结果
    resultDS
  }
}
