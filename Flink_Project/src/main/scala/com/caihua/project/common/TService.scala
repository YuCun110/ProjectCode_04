package com.caihua.project.common

import java.text.SimpleDateFormat

import com.caihua.project.bean.{ServiceLog, UserBehavior}
import com.caihua.project.dao.HotItemAnalysesDao
import org.apache.flink.streaming.api.scala._

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 通用服务
 */
trait TService {
  /**
   * 获取持久层对象
   * @return
   */
  def getDao(): TDao

  /**
   * 统计分析
   * @return
   */
  def analyses(): Any

  /**
   * 获取用户行为数据
   * @return
   */
  def getUserBehavir() ={
    //1.读取数据
    val dataDS: DataStream[String] = getDao().readTextFile("input/UserBehavior.csv")

    //2.转换数据结构，拆分数据，封装为样例类对象
    val userBehaviorDS: DataStream[UserBehavior] = dataDS.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(
        arr(0).toLong,
        arr(1).toLong,
        arr(2).toInt,
        arr(3).toString,
        arr(4).toLong)
    })

    //3.返回数据读取结果
    userBehaviorDS
  }

  def getServiceLog() ={
    //1.读取数据
    val dataDS: DataStream[String] = getDao().readTextFile("input/apache.log")

    //2.变换数据结构，拆分数据，封装为样例类对象
    val serviceLogDS: DataStream[ServiceLog] = dataDS.map(line => {
      //① 切分数据
      val arr: Array[String] = line.split(" ")
      //② 时间格式化
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      //③ 封装为样例类对象
      ServiceLog(
        arr(0),
        arr(1),
        sdf.parse(arr(3)).getTime,
        arr(5),
        arr(6)
      )
    })
    //3.返回数据
    serviceLogDS
  }
}
