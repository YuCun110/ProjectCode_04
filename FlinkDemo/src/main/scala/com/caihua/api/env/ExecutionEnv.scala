package com.caihua.api.env

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object ExecutionEnv {
  def main(args: Array[String]): Unit = {
    // TODO 创建Env（上下文环境）的几种方式
    // TODO 批处理
    //1. 批处理的上下文环境
    val envBatch: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.设定并行度(全局)
    envBatch.setParallelism(2)

    // TODO 流处理
    //1.流处理的上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设定并行度
    envStream.setParallelism(2)

    // TODO 获取上下文环境对象
    //动态识别环境的信息，判断是否创建本地环境还是集群坏境
    ExecutionEnvironment.getExecutionEnvironment

    //创建本地环境对象，并设置并行度
    ExecutionEnvironment.createLocalEnvironment(2)

    //创建远程环境（集群）对象
    val host = "hadoop202" //集群IP地址
    val port = 6123 //端口号
    val env: ExecutionEnvironment = ExecutionEnvironment.createRemoteEnvironment(host, port,
      "jar包路径")

  }
}