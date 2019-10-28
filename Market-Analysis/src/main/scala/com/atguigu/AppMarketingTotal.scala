package com.atguigu

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulateSource)
      .assignAscendingTimestamps(_.timestamp)
    //dataStream.print("data")
    dataStream
      .filter(_.behavior != "UNINSTALL")
      .map(_ => ("total", 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new MarketingCountByChannel())
      .print()

    env.execute("app marketing total job")
  }

}

class MarketingCountByChannel() extends ProcessWindowFunction[(String, Long), MarketingViewCount,String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[MarketingViewCount]): Unit = {
    out.collect(MarketingViewCount(context.window.getEnd.toString, key, key, elements.size))
  }
}

class SimulateSource() extends SourceFunction[MarketingUserBehavior] {

  var running: Boolean = true
  val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL", "UPDATE")
  val channelSet: Seq[String] = Seq("APPLE STORE", "HUAWEI STORE", "XIAOMI STORE", "WEIBO", "WECHAT")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {

    val maxElements: Long = Long.MaxValue
    var count: Long = 0L

    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behaviorType: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel: String = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behaviorType, channel, ts))
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }


  }

  override def cancel(): Unit = running = false
}