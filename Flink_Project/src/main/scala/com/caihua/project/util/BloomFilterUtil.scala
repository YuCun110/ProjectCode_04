package com.caihua.project.util

/**
 * TODO 布隆过滤器模拟：
 * 1.使用Redis作为位图：key(id) field(offset) value(0,1)
 * 2.使用当前对象对位图进行定位(偏移量的计算)
 *
 * @author XiLinShiShan
 * @version 0.0.1
 */
object BloomFilterUtil {
  // TODO Redis中位图最大容量为：512M * 1024 * 1024 * 8
  //1.设置位图的容量
  private val capacity = 1 << 29

  /**
   * 计算偏移量
   *
   * @param value
   * @param seed
   */
  def getOffset(value: String, seed: Int) = {
    // TODO HashMap离散化的方式：hash(key.hashCode) & (length - 1 ) => index
    //      Redis中slot的离散化：crc(key.hashCode) & (16384 - 1) => slot
    //TODO 离散化偏移量
    //1.定义hash值
    var hash = 0

    //2.循环遍历字符串中的字符，计算hash值
    for (c <- value) {
      hash = hash*seed + c
    }

    //3.偏移量：将计算后的hash值进行散列
    hash & (capacity - 1)
 }

  def main(args: Array[String]): Unit = {
    println(getOffset("hello",5))
    println(getOffset("hello",5))
  }
}
