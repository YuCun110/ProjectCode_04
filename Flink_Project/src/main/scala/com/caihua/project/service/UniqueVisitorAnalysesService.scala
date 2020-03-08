package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.UniqueVisitorAnalysesDao
import com.caihua.project.function.{UniqueVisitorProcessFunction, UniqueVisitorProcessFunctionByBloomFilter}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class UniqueVisitorAnalysesService extends TService{
  /**
   * 获取持久层对象
   *
   * @return
   */
  override def getDao(): TDao = new UniqueVisitorAnalysesDao

  /**
   * 统计分析
   *
   * @return
   */
  override def analyses() = {
    //1.获取原始数据
    val userBehaviorDS: DataStream[bean.UserBehavior] = getUserBehavir()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000)

    //3.变换数据结构
    val tupleDS: DataStream[(Long, Int)] = markDS.map(user => (user.userId,1))

    //4.开窗：一个小时滚动
    val windowWS: AllWindowedStream[(Long, Int), TimeWindow] = tupleDS.timeWindowAll(Time.hours(1))

    //5.按照用户ID去重，统计用户访问量
    val resultDS: DataStream[String] = windowWS.process(
      new UniqueVisitorProcessFunction
    )

    //6.返回结果
    resultDS
  }

  /**
   * 统计分析，通过bloom过滤器对数据进行去重
   *
   * @return
   */
  def analysesByBloomFilter() ={
    //1.获取原始数据
    val userBehaviorDS: DataStream[bean.UserBehavior] = getUserBehavir()

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000)

    //3.变换数据结构
    val tupleDS: DataStream[(Long, Int)] = markDS.map(user => (user.userId,1))

    //4.开窗：一个小时滚动
    val windowWS: AllWindowedStream[(Long, Int), TimeWindow] = tupleDS.timeWindowAll(Time.hours(1))

    // TODO 在使用去重功能时，希望不要将所有数据放入内存中计算，否则容易OOM
    //      1.使用process，处理全量数据，不适合；
    //      2.使用aggregate，一次处理一个数据，但是该方法主要功能为累加器，不做业务逻辑处理
    //      3.可以结合process和window的触发器使用

    // TODO .trigger()触发器：定义 window 什么时候关闭，触发计算并输出结果
    //5.按照用户ID去重，统计用户访问量
    val resultDS: DataStream[String] = windowWS.trigger(
      new Trigger[(Long, Int), TimeWindow]() {
        //针对一个元素触发
        override def onElement(element: (Long, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {
          //处理完元素后，将数据从窗口中清除
          TriggerResult.FIRE_AND_PURGE
        }

        //针对处理时间触发
        override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {
          TriggerResult.CONTINUE
        }

        //针对事件时间触发
        override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {
          TriggerResult.CONTINUE
        }

        //关闭资源
        override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = {}
      }
    ).process(
      new UniqueVisitorProcessFunctionByBloomFilter
    )

    //6.返回结果
    resultDS
  }
}
