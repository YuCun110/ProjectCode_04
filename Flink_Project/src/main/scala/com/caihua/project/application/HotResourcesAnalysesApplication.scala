package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.HotResourcesAnalysesController

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 统计热门资源的点击量排行
 */
object HotResourcesAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制器对象
    val controller = new HotResourcesAnalysesController

    //2.启动控制器
    controller.execute()
  }
}
