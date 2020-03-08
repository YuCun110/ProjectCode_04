package com.caihua.project.function

import com.caihua.project.util.BloomFilterUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
// TODO ProcessAllWindowFunction[IN, OUT, W <: Window]
class UniqueVisitorProcessFunctionByBloomFilter extends ProcessAllWindowFunction[(Long, Int), String, TimeWindow] {
  //1.定义Redis客户端连接对象
  private var redisClient: Jedis = _

  //2.初始化
  override def open(parameters: Configuration): Unit = {
    redisClient = new Jedis("hadoop202", 6379)
  }

  //3.一个一个元素，通过布隆过滤器，进行去重
  override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {
    //① 获取访问的用户id
    val userId: String = elements.iterator.next()._1.toString
    //② 定义位图中的Key，用来存放每个窗口的独立访客数
    val bitMapKey = context.window.getEnd.toString
    //③ 通过自定义布隆过滤器计算偏移量
    val offset: Int = BloomFilterUtil.getOffset(userId, 15)
    //④ 根据偏移量判断用户id是否在Redis位图中
    if (redisClient.getbit(bitMapKey, offset)) {
      //存在，不做处理
    } else {
      //位图中不存在该用户，跟新该用户的位置
      redisClient.setbit(bitMapKey,offset,true)
      //获取独立访客数
      val lastCount: String = redisClient.hget("UV_Count",bitMapKey)
      //判断访客数是否存在
      if(lastCount != null && lastCount != ""){
        redisClient.hset("UV_Count",bitMapKey,(lastCount.toLong + 1).toString)
      }else{
        redisClient.hset("UV_Count",bitMapKey,"1")
      }
    }
    Thread.sleep(1000)
    //⑤ 输出信息
    out.collect(s"当前时间：${context.window.getEnd}，新增加访客：$userId")
  }
}
