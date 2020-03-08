package com.caihua.project.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object FlinkStreamEnv {
  //1.在同一个线程中，在一个内存中创建上下文环境变量（该线程内共享）
  private val envStream = new ThreadLocal[StreamExecutionEnvironment]

  /**
   * 初始化Flink上下文环境
   */
  def init()={
    //① 创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //② 设置并行度
    env.setParallelism(1)
    //③ 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //④ 将运行环境放入共享内存中
    envStream.set(env)
  }

  /**
   * 获取上下文环境
   */
  def getEnv()={
    //① 判断运行环境是否为空
    if(envStream.get() == null){
      init()
    }
    //② 返回运行环境
    envStream.get()
  }

  /**
   * 启动任务的执行
   */
  def execute(): Unit ={
    getEnv().execute("Application")
  }

  /**
   * 清除环境
   */
  def clear(): Unit ={
    envStream.remove()
  }
}
