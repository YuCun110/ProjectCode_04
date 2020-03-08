package com.caihua.api.table_sql

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.{StreamTableEnvironment,_}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Table_Sql {
  def main(args: Array[String]): Unit = {
    // TODO Table API

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    envStream.setParallelism(1)

    //3.读取端口数据，并创建DS
    val socketDS: DataStream[String] = envStream.socketTextStream("hadoop202",9999)

    //4.变换数据结构
    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(line => {
      val arr: Array[String] = line.split(",")
      WaterSensor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // TODO 获取TableAPI环境
    //5.获取TableAPI环境
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(envStream)

    //6.将数据转换为一张表(过滤)
    val table: Table = tableEnv.fromDataStream(waterSensorDS,'id,'ts)

    //7.筛选数据
    //导入隐式转换
    val result: DataStream[String] = table.select("id").toAppendStream[String]

    result.print()

    //8.启动任务
    envStream.execute()
  }

}
