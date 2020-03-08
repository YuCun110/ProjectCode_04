package com.caihua.api.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.caihua.api.udf.MyRichMap
import com.caihua.bean.WaterSensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Sink_JDBC {
  def main(args: Array[String]): Unit = {
    // TODO JDBC自定义Sink
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.从集合中读取数据，创建DS
    //导入隐式转换
    val dataDS: DataStream[(Int, String)] = envStream.fromCollection(
      Seq((19, "zhangsan"),
        (19, "zhangsan")
      ))

    //3.将数据写入MySQL数据库
    dataDS.addSink(new MyJdbcSink)

    //4.执行任务
    envStream.execute()
  }
}

class MyJdbcSink extends RichSinkFunction[(Int,String)]{
  var connection: Connection = _
  var statement: PreparedStatement =_

  //1.创建连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    connection = DriverManager.getConnection(
      "jdbc:mysql://hadoop202:3306/test",
      "root",
      "123456"
    )
    statement = connection.prepareStatement("INSERT INTO person VALUES(?,?)")

  }

  //调用连接，执行Sql
  override def invoke(value: (Int,String), context: SinkFunction.Context[_]): Unit = {
    statement.setInt(1,value._1)
    statement.setString(2,value._2)
    statement.execute()
  }

  //关闭资源
  override def close(): Unit = {
    statement.close()
    connection.close()
  }
}