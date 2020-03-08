package com.caihua.api.sink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Sink_Redis {
  def main(args: Array[String]): Unit = {
    // TODO Sink：将数据写入Redis数据库 RedisSink
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读取数据，创建DS
    //导入隐式转换
    val dataDS: DataStream[(Int, String, Double)] = envStream.fromCollection(Seq((1001,"zhangsan",87.7),(1002,"lisi",98.3),(1003,"wangwu",90.0)))

    //3.将数据存入Redis中
    //① 设置Redis配置参数
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop202").setPort(6379).build()
    //②使用addSink
    dataDS.addSink(new RedisSink[(Int, String, Double)](
      config,
      new RedisMapper[(Int, String, Double)] {
        //a.设置数据类型和RedisKey
        override def getCommandDescription = {
          //将数据设置为Hash
          new RedisCommandDescription(RedisCommand.HSET,"flink_redis")
        }

        //b.设置HashKey
        override def getKeyFromData(t: (Int, String, Double)) = {
          t._1.toString
        }

        //c.设置Value
        override def getValueFromData(t: (Int, String, Double)) = {
          t.toString()
        }
      }
    ))

    //4.启动任务
    envStream.execute()
  }
}
