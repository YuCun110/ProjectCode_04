package com.caihua.project.function

import com.caihua.project.bean.LoginEvent
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
class LoginFailProcessFunction extends KeyedProcessFunction[Long,LoginEvent,String]{
  //定义键控状态变量
  //1.定义上一次登录失败的时间
  private var lastLoginFailTimestamp: ValueState[Long] = _

  //2.初始化状态变量
  override def open(parameters: Configuration): Unit = {
    lastLoginFailTimestamp = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("lastLoginFailTimestamp",classOf[Long])
    )
  }

  //3.遍历每一个元素，判断是否连续两秒登陆两次失败
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]): Unit = {
    if(value.eventTime - lastLoginFailTimestamp.value() <= 2){
      out.collect(s"用户：${value.userId}，连续两秒登录两次失败！")
    }
    lastLoginFailTimestamp.update(value.eventTime)
  }
}
