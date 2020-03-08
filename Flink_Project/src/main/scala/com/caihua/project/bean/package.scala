package com.caihua.project

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
package object bean {

  /**
   * 用户行为
   * @param userId 加密后的用户ID
   * @param itemId 加密后的商品ID
   * @param categoryId 加密后的商品所属类别ID
   * @param behavior 用户行为类型，包括(‘pv’, ‘’buy, ‘cart’, ‘fav’)
   * @param timestamp 行为发生的时间戳，单位秒
   */
  case class UserBehavior(
       userId: Long,
       itemId: Long,
       categoryId: Int,
       behavior: String,
       timestamp: Long)

  /**
   * 对统计后的所有商品点击量结果，增加该商品所在的窗口结束时间
   * @param itemId
   * @param clickCount
   * @param windowEnd
   */
  case class HotItemClick(
       itemId: Long,
       clickCount: Long,
       windowEnd: Long)

  /**
   * 服务器日志对象
   * @param ip
   * @param userId
   * @param eventTime
   * @param method
   * @param url
   */
  case class ServiceLog(
        ip: String,
        userId: String,
        eventTime: Long,
        method: String,
        url: String)

  /**
   * 热门资源对象
   * @param url
   * @param clickCount
   * @param WindowEndTime
   */
  case class HotResourceClick(
       url:String,
       clickCount: Long,
       WindowEndTime: Long)
}
