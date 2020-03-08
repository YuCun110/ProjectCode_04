package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.HotResourcesAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class HotResourcesAnalysesController extends TController{
  //1.创建服务层对象
  private val service = new HotResourcesAnalysesService

  override def execute(): Unit = {
    //1.获取统计分析结果
    val result: DataStream[String] = service.analyses()

    //2.打印输出
    result.print()
  }
}
