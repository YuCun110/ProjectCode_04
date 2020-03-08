package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.AdvertisementClickAnalysesController

/**
 * 页面广告点击量统计(按照省份划分)
 * @author XiLinShiShan
 * @version 0.0.1
 */
object AdvertisementClickAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制层对象
    val controller = new  AdvertisementClickAnalysesController

    //2.启动程序
    controller.execute()
  }
}
