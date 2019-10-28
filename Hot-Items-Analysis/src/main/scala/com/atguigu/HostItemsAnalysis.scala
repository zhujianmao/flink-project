package com.atguigu

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector


case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
  * 每隔5分钟输出最近一小时内点击量最多的前N个商品
  */
object HostItemsAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置event-time语义
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


      //    val url: String = "E:\\code\\idea\\User-Behavior-Analysis\\Hot-Items-Analysis\\src\\main\\resources\\UserBehavior.csv"
//    val inputDataStream: DataStream[UserBehavior] = env.readTextFile(url)
     val inputDataStream: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      .map(line => {
        val splits: Array[String] = line.split(",")
        UserBehavior(splits(0).toLong, splits(1).toLong, splits(2).toInt, splits(3), splits(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //设置水印

    val aggDataStream: DataStream[ItemViewCount] = inputDataStream.filter(_.behavior == "pv") //过滤出pv类型了日志
      .keyBy(_.itemId) //根据产品id分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //设置滑窗
      .aggregate(new CountAgg(), new WindowViewCount())

    val topNDataStream: DataStream[String] = aggDataStream.keyBy(_.windowEnd)
      .process(new TopNProcessFunction(3))

    //aggDataStream.print("agg")
    topNDataStream.print()

    env.execute("HostItemsAnalysis")
  }

}

class TopNProcessFunction(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  var listItems: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    listItems = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("list-items", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    listItems.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    import scala.collection.JavaConversions._

    val topNItemViesCounts: List[ItemViewCount] = listItems.get()
      .iterator()
      .toList.
      sortBy(_.count)(Ordering.Long.reverse)
      .take(topN)
    listItems.clear()

    val result: StringBuilder = new StringBuilder
    result.append("==================================").append("\n")
    val time: Timestamp = new Timestamp(timestamp)
    result.append("时间:").append(time).append("\n")
    topNItemViesCounts.foreach(itemViewCount => {
      result.append("item:").append(itemViewCount.itemId).append(", count:").append(itemViewCount.count).append(" \n")
    })
    result.append("==================================").append("\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

class WindowViewCount() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}