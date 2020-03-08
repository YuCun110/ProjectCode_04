package com.caihua.project.common

import com.caihua.project.util.FlinkStreamEnv

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
trait TApplication {
  //启动任务(控制抽象)
  def start(op: => Unit): Unit ={
    try{
      //1.初始化Flink的运行环境
      FlinkStreamEnv.init()

      //2.执行代码逻辑
      op

      //3.启动任务
      FlinkStreamEnv.execute()
    }catch {
      case e => e.printStackTrace()
    }finally {
      //4.清除运行环境
      FlinkStreamEnv.clear()
    }
  }
}
