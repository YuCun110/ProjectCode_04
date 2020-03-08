package com.caihua.project.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object FlinkStreamContextUtil {
  //1.创建上下文环境对象
  private val envStream = new  ThreadLocal[StreamExecutionEnvironment]

  /**
   * 初始化上下文环境
   */
  def init(): Unit ={
    if(envStream.get() == null){
      //1.创建Fink上下文环境对象
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      //2.设置并行度
      env.setParallelism(1)
      //3.定义时间语义
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      //4.添加运行环境到线程共享内存
      envStream.set(env)
    }
  }

  def getEnv() ={
    //1.判断运行环境是否为空
    if(envStream.get() == null){
      init()
    }
    //2.返回运行环境
    envStream.get()
  }

  /**
   * 启动任务
   */
  def execute(): Unit ={
    envStream.get().execute()
  }

  /**
   * 清除环境信息
   */
  def clear(): Unit ={
    envStream.remove()
  }

}
