package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.AppMarketAnalysesController

/**
 * 统计所有市场中APP的下载、安装量
 * @author XiLinShiShan
 * @version 0.0.1
 */
object AppMarketAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制器对象
    val controller = new AppMarketAnalysesController

    //2.执行程序
    controller.execute()
  }
}
