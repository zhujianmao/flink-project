package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 网站独立访客数（UV）的统计
  */
object UniqueVisitAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置event-time语义
    env.setParallelism(1)

    val url: String = "E:\\code\\idea\\User-Behavior-Analysis\\Network-Analysis\\src\\main\\resources\\UserBehavior.csv"
    val inputDataStream: DataStream[UserBehavior] = env.readTextFile(url)
      .map(line => {
        val splits: Array[String] = line.split(",")
        UserBehavior(splits(0).toLong, splits(1).toLong, splits(2).toInt, splits(3), splits(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //设置水印

    inputDataStream.filter(_.behavior == "pv")
      .map(data => ("pv",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process( new UVProcessWindow())
      .print("sum")

    env.execute("unique visit analysis job")

  }

}

// 大量数据去重的话,容易发生OOM,建议使用布隆过滤器
class UVProcessWindow() extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
     var result: Set[Long] = Set[Long]()
      elements.iterator.foreach{
        case (_,userId) => result += userId
      }
    out.collect(UvCount(context.window.getEnd,result.size))
  }
}

case class UvCount(windowEnd: Long, count: Long)