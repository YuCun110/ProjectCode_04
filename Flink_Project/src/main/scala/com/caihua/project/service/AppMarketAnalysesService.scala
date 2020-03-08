package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.AppMarketAnalysesDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class AppMarketAnalysesService extends TService{
  //1.创建持久层对象
  private val dao = new AppMarketAnalysesDao
  /**
   * 获取持久层对象
   *
   * @return
   */
  override def getDao(): TDao = dao

  /**
   * 统计分析：分渠道统计
   *
   * @return
   */
  override def analyses() = {
    //1.获取市场上App的数据
    val dataDS: DataStream[bean.MarketingUserBehavior] = dao.mockData()

    //2.抽取事件时间戳，并设置水位线
    val markDS: DataStream[bean.MarketingUserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp)

    //3.变换数据结构，并按照渠道和用户行为进行分组
    val groupByKS: KeyedStream[((String, String), Long), (String, String)] = markDS.map(behavior => ((behavior.channel, behavior.behavior), 1L))
      .keyBy(_._1)

    //4.开窗聚合：2分钟滚动，5秒滑动
    val resultDS: DataStream[String] = groupByKS.timeWindow(Time.minutes(2), Time.seconds(5))
      .process(
        new ProcessWindowFunction[((String, String), Long), String, (String, String), TimeWindow] {
          override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[String]): Unit = {
            //统计各个渠道各个行为的数量，并输出
            out.collect(s"当前时间段：[${context.window.getStart}-${context.window.getEnd}]，渠道和操作：[${elements.iterator.next()._1}],用户数：[${elements.size}]")
          }
        }
      )

    //5.输出
    resultDS
  }

  /**
   * 统计分析：不分渠道统计
   *
   * @return
   */
  def analysesNoChannel() = {
    //1.获取市场上App的数据
    val dataDS: DataStream[bean.MarketingUserBehavior] = dao.mockData()

    //2.抽取事件时间戳，并设置水位线
    val markDS: DataStream[bean.MarketingUserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp)

    //3.变换数据结构，并按照用户行为进行分组
    val groupByKS: KeyedStream[(String, Long), String] = markDS.map(behavior => (behavior.behavior, 1L))
      .keyBy(_._1)

    //4.开窗聚合：2分钟滚动，5秒滑动
    val resultDS: DataStream[String] = groupByKS.timeWindow(Time.minutes(2), Time.seconds(5))
      .process(
        new ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
            out.collect(s"当前时间段：[${context.window.getStart}-${context.window.getEnd}]，" +
              s"用户行为：[${elements.iterator.next()._1}]，" +
              s"数量：[${elements.size}]")
          }
        })

    //5.输出
    resultDS
  }
}
