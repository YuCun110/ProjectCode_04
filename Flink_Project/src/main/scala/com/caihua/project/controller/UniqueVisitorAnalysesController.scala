package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.UniqueVisitorAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class UniqueVisitorAnalysesController extends TController{
  //1.创建服务层对象
  private val service = new UniqueVisitorAnalysesService

  /**
   * 执行任务
   */
  override def execute(): Unit = {
    //1.获取统计分析结果
    // TODO 使用Redis中的Set去重
    //val result: DataStream[String] = service.analyses()
    // TODO 使用模拟的布隆过滤器去重
    val result: DataStream[String] = service.analysesByBloomFilter()

    //2.打印输出
    result.print()
  }
}
