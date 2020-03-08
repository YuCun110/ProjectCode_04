package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.PageViewAnalysesController

/**
 * @author XiLinShiShan
 * @version 0.0.1
 *          统计每个小时网站的总浏览量
 */
object PageViewAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制器对象
    val controller = new PageViewAnalysesController

    //2.启动程序运行
    controller.execute()
  }
}
