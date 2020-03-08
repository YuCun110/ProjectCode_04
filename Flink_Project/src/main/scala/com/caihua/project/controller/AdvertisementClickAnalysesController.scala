package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.AdvertisementClickAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class AdvertisementClickAnalysesController extends TController{
  //1.创建服务层对象
  private val service = new  AdvertisementClickAnalysesService
  /**
   * 执行任务
   */
  override def execute(): Unit = {
    //1.获取分析结果
    // 没有设置黑名单
    //val result = service.analyses()
    // 设置黑名单
    val result: DataStream[String] = service.analysesWithBlackList()

    //2.打印输出
    result.print
  }
}
