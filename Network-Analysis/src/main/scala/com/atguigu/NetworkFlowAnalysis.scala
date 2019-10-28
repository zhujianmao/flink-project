package com.atguigu

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Map.Entry

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

/**
  * 每隔5秒，输出最近10分钟内访问量最多的前N个URL。
  */
object NetworkFlowAnalysis {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //    val fileUrl: String = "E:\\code\\idea\\User-Behavior-Analysis\\Network-Analysis\\src\\main\\resources\\apache.log"
    //93.114.45.13 - - 17/05/2015:10:05:14 +0000 GET /articles/dynamic-dns-with-dhcp/
    //  val dataStream: DataStream[ApacheLogEvent] = env.readTextFile(fileUrl)
    val dataStream = env.socketTextStream("hadoop102", 7777)
      .map(line => {
        val splits: Array[String] = line.split(" ")
        val timestamp: Long = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(splits(3)).getTime
        ApacheLogEvent(splits(0), splits(1), timestamp, splits(5), splits(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    val aggStream: DataStream[UrlViewCount] = dataStream
      //      .filter(data => {
      //        val pattern = "^((?!\\.(css|js|ico|png)$).)*$".r
      //        (pattern findFirstIn data.url).nonEmpty
      //      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .aggregate(new NetworkCountAgg, new ResultWindowFunction)

    val processStream: DataStream[String] = aggStream.keyBy(_.windowEnd)
      .process(new TopNProcessFunction(3))

    dataStream.print("data")
    aggStream.print("agg")
    processStream.print("process")

    env.execute(" network flow job")
  }
}

class TopNProcessFunction(topN: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  lazy val urlMapStat: MapState[String, Long] = getRuntimeContext
    .getMapState(new MapStateDescriptor[String, Long]("urlmap-state", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlMapStat.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val topNUrlList: List[Entry[String, Long]] = urlMapStat.entries()
      .iterator()
      .toList
      .sortWith((url1, url2) => {
        url1.getValue > url2.getValue
      })
      .take(topN)
    urlMapStat.clear()
    //写出结果
    val result: StringBuilder = new StringBuilder
    result.append("\n=========================================================\n")
    result.append("时间: ").append(new Timestamp(timestamp)).append("\n")

    for (i <- topNUrlList.indices) {
      val url: Entry[String, Long] = topNUrlList(i)
      result.append("第").append(i + 1).append("名:").append("\n")
      result.append("\t\t").append("URL: ").append(url.getKey).append("\n")
      result.append("\t\t").append("总数count: ").append(url.getValue).append("\n")
    }
    result.append("=========================================================\n")
    Thread.sleep(50)
    out.collect(result.toString())
  }
}

class ResultWindowFunction() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class NetworkCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
