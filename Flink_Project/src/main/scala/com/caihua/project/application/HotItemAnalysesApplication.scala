package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.HotItemAnalysesController

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 热门商品的统计分析
 */
object HotItemAnalysesApplication extends App with TApplication{
  //启动应用程序
  start{
    //1.创建控制器对象
    val controller = new HotItemAnalysesController

    //2.执行控制器
    controller.execute()
  }
}
