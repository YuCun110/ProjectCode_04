package com.caihua.api.watermark

import com.caihua.bean.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object Flink_EventTime {
  def main(args: Array[String]): Unit = {
    // TODO 时间语义：
    //  1）Event Time：是事件创建的时间。Flink通过时间戳分配器访问事件时间戳
    //  2）Ingestion Time：是数据进入Flink的时间
    //  3）Processing Time：是每一个执行基于时间操作的算子的本地系统时间，与机器相关，
    //  默认的时间属性就是Processing Time，Event Time最有意义
    //  绝大部分的业务都会使用EventTime

    //1.创建上下文环境
    val envStream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStream.setParallelism(1)

    //TODO Event Time的引入：通过上下文环境设置
    //2.设置时间语义
    envStream.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  }
}
