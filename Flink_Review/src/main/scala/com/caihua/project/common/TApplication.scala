package com.caihua.project.common

import com.caihua.project.util.FlinkStreamContextUtil

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
trait TApplication {
  /**
   * 启动程序(使用控制抽象)
   */
  def start(op: => Unit): Unit ={
    try{
      //1.获取上下文环境
      FlinkStreamContextUtil.getEnv()

      //2.执行主体代码
      op

      //3.启动任务
      FlinkStreamContextUtil.execute()
    }catch {
      case e => e.printStackTrace()
    }finally {
      //4.清除运行环境
      FlinkStreamContextUtil.clear()
    }
  }

}
