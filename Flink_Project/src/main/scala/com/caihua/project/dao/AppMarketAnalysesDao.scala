package com.caihua.project.dao

import com.caihua.project.bean.MarketingUserBehavior
import com.caihua.project.common.TDao
import com.caihua.project.util.FlinkStreamEnv
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class AppMarketAnalysesDao extends TDao {
  /**
   * 模拟生成市场推广的数据
   */
  def mockData() = {
    //1.获取上下文环境
    val env: StreamExecutionEnvironment = FlinkStreamEnv.getEnv()

    //2.创建自定义数据源
    env.addSource(
      new SourceFunction[MarketingUserBehavior] {
        //① 定义标记
        var isRun = true
        //② 定义用户行为
        val behaviorlist = List("INSTALL", "DOWNLOAD", "CLICK")
        //③ 定义市场渠道
        val channelList = List("HUAWEI","XIAOMI","IPHONE","OPPO")

        override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]) = {
          while (isRun) {
            //模拟原始数据
            ctx.collect(MarketingUserBehavior(Random.nextInt(100),
              behaviorlist(Random.nextInt(3)),
              channelList(Random.nextInt(4)),
              System.currentTimeMillis()))
            //时间延迟
            Thread.sleep(500)
          }
        }

        override def cancel() = {
          isRun = false
        }
      }
    )
  }
}
