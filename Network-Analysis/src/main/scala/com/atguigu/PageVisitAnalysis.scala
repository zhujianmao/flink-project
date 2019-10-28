package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/**
  * 网站总浏览量（PV）的统计
  *
  * 设置滚动时间窗口，实时统计每小时内的网站PV
  */
object PageVisitAnalysis {
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
      .map(_ => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)
      .print("sum")

  env.execute("page view analysis job")
  }

}
