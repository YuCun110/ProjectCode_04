package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.PageViewAnalysesDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class PageViewAnalysesService extends TService{
  /**
   * 获取持久层对象
   * @return
   */
  override def getDao(): TDao = new PageViewAnalysesDao

  /**
   * 统计分析
   * @return
   */
  override def analyses() = {
    //1.获取数据
    val userBehavirDS: DataStream[bean.UserBehavior] = getUserBehavir()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.UserBehavior] = userBehavirDS.assignAscendingTimestamps(_.timestamp * 1000)

    //3.过滤：只选取日志行为为“pv”的数据
    val filterDS: DataStream[bean.UserBehavior] = markDS.filter(_.behavior == "pv")

    //4.变换数据结构
    val tupleDS: DataStream[(String, Int)] = filterDS.map(user => (user.behavior,1))

    //5.对所有数据进行开窗：滚动窗口1小时
    val windowWS: AllWindowedStream[(String, Int), TimeWindow] = tupleDS.timeWindowAll(Time.hours(1))

    //6.聚合：求浏览页面的总人数
    val resultDS: DataStream[(String, Int)] = windowWS.sum(1)

    //7.返回统计结果
    resultDS
  }
}
