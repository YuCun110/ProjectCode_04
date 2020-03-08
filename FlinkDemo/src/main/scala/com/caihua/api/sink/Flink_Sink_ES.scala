package com.caihua.api.sink

import java.util

import com.caihua.bean.WaterSensor
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_Sink_ES {
  def main(args: Array[String]): Unit = {
    //TODO Sink：将数据写入ElasticSearch
    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读取数据
    //导入隐式转换
    val waterSensorDS: DataStream[WaterSensor] = envStream.fromCollection(Seq(WaterSensor("1001", System.currentTimeMillis(), Random.nextDouble()),
      WaterSensor("1002", System.currentTimeMillis(), Random.nextDouble()),
      WaterSensor("1003", System.currentTimeMillis(), Random.nextDouble()))
    )

    //3.写入ElasticSearch
    //① 创建连接地址
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop202", 9200))
    //② 完善配置信息
    val esSinkBuilder = new ElasticsearchSink.Builder[WaterSensor](
      httpHosts,
      new ElasticsearchSinkFunction[WaterSensor] {
        override def process(t: WaterSensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)
          val json = new java.util.HashMap[String, String]()
          json.put("data", t.toString)
          val indexRequest = Requests.indexRequest().index("flink_es").`type`("readingData").source(json)
          requestIndexer.add(indexRequest)
          println("saved successfully")
        }
      }
    )
    //执行写入
    waterSensorDS.addSink(esSinkBuilder.build())

    //4.启动任务
    envStream.execute()
  }
}
