package com.caihua.project.common

import com.caihua.project.util.FlinkStreamEnv

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 通用数据访问
 */
trait TDao {
  /**
   * 从文件读取数据
   */
  def readTextFile(implicit path: String)={
    FlinkStreamEnv.getEnv().readTextFile(path)
  }

  /**
   * 从Kafka读取数据
   */
  def readKafka()={

  }

  /**
   * 从端口读取数据
   */
  def readSocket(): Unit ={

  }

}
