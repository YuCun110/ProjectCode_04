package com.caihua.project.application

import com.caihua.project.common.TApplication
import com.caihua.project.controller.LoginFailAnalysesController

/**
 * 对用户的登录失败动作进行统计，在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
 * @author XiLinShiShan
 * @version 0.0.1
 */
object LoginFailAnalysesApplication extends App with TApplication{
  start{
    //1.创建控制器
    val controller = new LoginFailAnalysesController

    //2.执行程序
    controller.execute()
  }
}
