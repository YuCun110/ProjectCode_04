package com.caihua.project.common

import com.caihua.project.bean.UserBehavior
import com.caihua.project.util.FlinkStreamContextUtil
import org.apache.flink.streaming.api.scala._

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
trait TDao {
  /**
   * 通过本地文件读取数据
   */
  def readTextFile(implicit path:String) ={
    //1.读取数据
    val dataDS: DataStream[String] = FlinkStreamContextUtil.getEnv().readTextFile(path)
    //2.返回数据
    dataDS
  }

  def getUserBehavior() ={
    //1.读取本地文件数据
    val dataDS: DataStream[String] = readTextFile("input/UserBehavior.csv")

    //2.变换数据类型，将数据进行拆分，并封装为样例类对象
    val userBehaviorDS: DataStream[UserBehavior] = dataDS.map(line => {
      val arr: Array[String] = line.split(",")
      UserBehavior(
        arr(0).toLong,
        arr(1).toLong,
        arr(2).toInt,
        arr(3),
        arr(4).toLong
      )
    })

    //3.返回结果集
    userBehaviorDS
  }

}
