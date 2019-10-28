package com.atguigu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

/**
  * 主函数中先以province进行keyBy，
  * 然后开一小时的时间窗口，滑动距离为5秒，统计窗口内的点击事件数量
  */
object AdsClickStatistics {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val filePath: String = getClass.getResource("/AdClickLog.csv").getPath
    val dataStream: DataStream[AdClickLog] = env.readTextFile(filePath)
      .map(data => {
        val splits: Array[String] = data.split(",")
        AdClickLog(splits(0).toLong, splits(1).toLong, splits(2), splits(3), splits(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000)

    val resultStream: DataStream[CountByProvince] = dataStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new CountAdsClickAgg(), new CountResultByProvince())

    resultStream.print("result")
    env.execute("ads click statistics")
  }

}

class CountResultByProvince() extends ProcessWindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(context.window.getEnd.toString,key,elements.iterator.next()))
  }
}

class CountAdsClickAgg() extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}