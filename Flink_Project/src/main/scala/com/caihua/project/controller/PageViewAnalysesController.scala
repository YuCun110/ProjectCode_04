package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.PageViewAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class PageViewAnalysesController extends TController{
  //1.创建服务层对象
  private val service = new PageViewAnalysesService

  override def execute(): Unit = {
    //1.统计网站总浏览量
    val result: DataStream[(String, Int)] = service.analyses()

    //2.打印输出
    result.print()
  }
}
