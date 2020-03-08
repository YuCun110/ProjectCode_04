package com.caihua.api.dataflow

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_DataFlow_01 {
  def main(args: Array[String]): Unit = {
    //TODO 任务链：相同并行度的One to One(窄依赖) 操作，Flink将这样的算子链接在一起形成一个Task
    //1.创建上下文环境（有界流）
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // TODO ：禁用操作链(一个SubTask就是一个Task)
    // 全局的算子无论并行度是否相同，是否为窄依赖，均单独成为一个任务，不会成为一个操作链
    env.disableOperatorChaining()
    //2.设置并行度
    env.setParallelism(2)

    //3.读取数据
    val linesDS: DataStream[String] = env.readTextFile("input")

    //4.扁平化：拆分单词
    //导入隐式转换
    import org.apache.flink.api.scala._
    val wordToOneDS: DataStream[String] = linesDS.flatMap(_.split(","))

    //5.变换数据类型
    val wordToTupleDS: DataStream[(String, Int)] = wordToOneDS.map((_,1))

    //6.统计单词频数
    val wordCountDS: DataStream[(String, Int)] = wordToTupleDS.keyBy(_._1).sum(1)

    //7.打印输出
    wordCountDS.print("WordCount：")

    //8.启动任务
    env.execute()
  }
}
