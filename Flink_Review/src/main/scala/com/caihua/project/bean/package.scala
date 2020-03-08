package com.caihua.project

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
package object bean {

  /**
   * 描述用户行为的类
   * @param userId
   * @param itemID
   * @param categoryId
   * @param behavior
   * @param timestamp
   */
  case class UserBehavior(
       userId: Long,
       itemID: Long,
       categoryId: Int,
       behavior: String,
       timestamp: Long)

  /**
   * 描述服务器日志数据的类
   * @param ip
   * @param userId
   * @param eventTime
   * @param method
   * @param url
   */
  case class ServiceLog(
       ip:String,
       userId: String,
       eventTime: Long,
       method: String,
       url: String)

  /**
   * 热门商品信息类
   * @param itemId
   * @param clickCount
   * @param windowEndTime
   */
  case class HotItemInfo(
      itemId: Long,
      clickCount: Long,
      windowEndTime: Long)

  /**
   * 热门资源信息类
   * @param url
   * @param clickCount
   * @param windowEndTime
   */
  case class HotResourceClick(
        url: Long,
        clickCount: Long,
        windowEndTime: Long)
}
