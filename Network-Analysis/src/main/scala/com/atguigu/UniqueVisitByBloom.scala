package com.atguigu

import java.lang

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UniqueVisitByBloom {
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
      .map(data => ("pv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UVProcessByBloom())
      .print("sum")

    env.execute("unique visit analysis by bloom job")

  }

}


//定义布隆过滤器
class Bloom(size: Long) {
  private val cap: Long = size

  def hash(value: String, seed: Long): Long = {
    var result: Long = 0L
    for (i <- 0 until value.length) {
      // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result
  }

}

//定义process window function 函数
class UVProcessByBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  lazy private val jedis: Jedis = new Jedis("hadoop102", 6379)
  lazy private val bloom: Bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //窗口结束位置,作为位图的key,作为hash表的key
    val storeKey: String = context.window.getEnd.toString
    var count: Long = 0L
    //数据放在hash表里,先从hash表取数据
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    //val userId: String = elements.last._2.toString
    val waitForRedis: List[(String, Long)] = elements.iterator.toList
    waitForRedis.foreach {
      case (_, userId) => {
        val offset: Long = bloom.hash(userId.toString, 61)
        //判断位图时候存在
        val isExist: lang.Boolean = jedis.getbit(storeKey, offset)
        if (!isExist) {
          jedis.setbit(storeKey, offset, true)
          count += 1
          jedis.hset("count", storeKey, count.toString)
        }
      }
    }
    out.collect(UvCount(storeKey.toLong, count))

  }
}

//定义触发器
import scala.collection.JavaConversions._

class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

  var cacheListState: ListState[String] = _

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    cacheListState = ctx.getPartitionedState(new ListStateDescriptor[String]("list-state", classOf[String]))
    val it: lang.Iterable[String] = cacheListState.get()
    if (it != null && (it.iterator().toList.size > 10000 || timestamp == window.maxTimestamp())) {
      cacheListState.clear()
      TriggerResult.FIRE_AND_PURGE
    } else {
      cacheListState.add(element._1)
      TriggerResult.CONTINUE
    }


  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    cacheListState.clear()
  }
}