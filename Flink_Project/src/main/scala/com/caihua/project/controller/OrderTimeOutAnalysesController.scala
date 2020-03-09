package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.OrderTimeOutAnalysesService

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class OrderTimeOutAnalysesController extends TController{
  //1.创建服务层对象
  private val service = new OrderTimeOutAnalysesService

  /**
   * 执行任务
   */
  override def execute(): Unit = {
    //1.获取分析结果
    val reslut = service.analyses()

    //2.打印输出
    reslut.print
  }
}
