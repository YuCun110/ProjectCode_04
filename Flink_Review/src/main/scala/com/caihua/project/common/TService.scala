package com.caihua.project.common

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
trait TService {
  /**
   * 获取持久层对象
   * @return
   */
  def getDao:TDao
  /**
   * 统计分析
   * @return
   */
  def analyses():Any
}
