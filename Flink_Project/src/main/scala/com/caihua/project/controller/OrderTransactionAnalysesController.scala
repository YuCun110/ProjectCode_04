package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.OrderTransactionAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class OrderTransactionAnalysesController extends TController {
  //1.创建服务层对象
  private val service = new OrderTransactionAnalysesService
  /**
   * 执行任务
   */
  override def execute(): Unit = {
    //1.获取统计结果
    // TODO 双流匹配
    //val result = service.analyses()
    // TODO 双流JOIN
    val result: DataStream[String] = service.analysesByJoin

    //2.打印输出
    result.print
  }
}
