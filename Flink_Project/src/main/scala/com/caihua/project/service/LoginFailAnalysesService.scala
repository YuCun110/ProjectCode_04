package com.caihua.project.service

import com.caihua.project.bean
import com.caihua.project.bean.LoginEvent
import com.caihua.project.common.{TDao, TService}
import com.caihua.project.dao.LoginFailAnalysesDao
import com.caihua.project.function.LoginFailProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class LoginFailAnalysesService extends TService{
  /**
   * 获取持久层对象
   *
   * @return
   */
  override def getDao(): TDao = new LoginFailAnalysesDao

  /**
   * 统计分析
   *
   * @return
   */
  override def analyses() = {
    //1.获取数据
    val dataDS: DataStream[bean.LoginEvent] = getLogEvent

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.LoginEvent] = dataDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[bean.LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: bean.LoginEvent) = {
          element.eventTime * 1000L
        }
      }
    )

    //3.按照用户ID进行分组
    val userIdKS: KeyedStream[bean.LoginEvent, Long] = markDS.keyBy(_.userId)

    // TODO 使用CEP：两秒内登录失败五次
    //4.定义规则
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
        .where(_.eventType == "fail")
//        .next("next")
//        .where(_.eventType == "fail")
        .times(5)
        .within(Time.seconds(2))
    // 两秒内登录失败两次
//      .where(_.eventType == "fail")
//      .next("next")
//      .where(_.eventType == "fail")
//      .times(5)
//      .within(Time.seconds(2))

    //5.应用规则
    val patternPS: PatternStream[LoginEvent] = CEP.pattern(userIdKS,pattern)

    //6.获取结果
    val resultDS: DataStream[String] = patternPS.select(map => map.toString())

    //7.输出报警
    resultDS
  }


  def analysesNormal() = {
    //1.获取数据
    val dataDS: DataStream[bean.LoginEvent] = getLogEvent

    //2.抽取事件时间戳，并设置水位线标记
    val markDS: DataStream[bean.LoginEvent] = dataDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[bean.LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: bean.LoginEvent) = {
          element.eventTime * 1000L
        }
      }
    )

    //3.在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
    val resultDS: DataStream[String] = markDS.filter(_.eventType == "fail")
      .keyBy(_.userId)
      .process(
        new LoginFailProcessFunction
      )

    //4.输出报警
    resultDS
  }

}
