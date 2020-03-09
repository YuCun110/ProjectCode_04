package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.OrderTimeOutAnalysesController

/**
 * 为了让用户更有紧迫感从而提高支付转化率，设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消
 * @author XiLinShiShan
 * @version 0.0.1
 */
object OrderTimeOutAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制层对象
    val controller = new OrderTimeOutAnalysesController

    //2.执行程序
    controller.execute()
  }

}
