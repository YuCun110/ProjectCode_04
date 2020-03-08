package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.HotItemAnalysesController

/**
 * @author XiLinShiShan
 * @version 0.0.1
 *          计算最热门Top N商品：每隔5分钟统计一次近一个小时每个商品的点击量
 */
object HotItemAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制器对象
    val controller = new HotItemAnalysesController

    //2.启动程序
    controller.execute
  }
}
