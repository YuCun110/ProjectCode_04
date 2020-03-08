package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.HotItemAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 *          热门商品分析控制器
 */
class HotItemAnalysesController extends TController {
  private val service = new HotItemAnalysesService

  override def execute(): Unit = {
    //1.分析商品信息数据
    val resultDS: DataStream[String] = service.analyses()

    //2.打印输出
    resultDS.print("result--")
  }
}
