package com.caihua.api.source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Source_File {
  def main(args: Array[String]): Unit = {
    //1.创建上下文环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    env.setParallelism(2)

    //3.从文件读取数据，创建DataSet
    val linesDS: DataSet[String] = env.readTextFile("input")
  }

}
