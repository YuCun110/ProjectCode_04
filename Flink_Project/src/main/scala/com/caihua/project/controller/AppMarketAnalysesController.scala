package com.caihua.project.controller

import com.caihua.project.bean
import com.caihua.project.common.TController
import com.caihua.project.service.AppMarketAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class AppMarketAnalysesController extends TController{
  //1.创建数据服务层对象
  private val service = new AppMarketAnalysesService

  /**
   * 执行任务
   */
  override def execute(): Unit = {
    //1.获取分析结果
    //分渠道统计
    //val result = service.analyses()
    //不分渠道统计
    val result: DataStream[String] = service.analysesNoChannel()

    //2.打印输出
    result.print()
  }
}
