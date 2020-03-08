package com.caihua.project.controller

import com.caihua.project.common.TController
import com.caihua.project.service.LoginFailAnalysesService

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class LoginFailAnalysesController extends TController{
  //1.创建服务层对象
  private val service = new LoginFailAnalysesService

  /**
   * 执行任务
   */
  override def execute(): Unit = {
    //1.获取统计结果
    val result = service.analyses()

    //2.打印输出
    result.print
  }
}
