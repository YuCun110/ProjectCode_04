package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.UniqueVisitorAnalysesController

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object UniqueVisitorAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制器对象
    val controller = new UniqueVisitorAnalysesController

    //2.执行控制器
    controller.execute()
  }
}
