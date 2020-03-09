package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.OrderTransactionAnalysesController

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object OrderTransactionAnalysesApplication extends App with TApplication {
  start{
    //1.创建控制层对象
    val controller = new OrderTransactionAnalysesController

    //2.执行程序
    controller.execute()
  }

}
