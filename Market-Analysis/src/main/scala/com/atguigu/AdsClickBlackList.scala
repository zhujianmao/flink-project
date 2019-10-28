package com.atguigu

import java.lang

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdsClickBlackList {

  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blacklist")

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
      .assignAscendingTimestamps(_.timestamp * 1000)

    val filterBlackListStream: DataStream[AdClickLog] = dataStream.keyBy(data => (data.userId,data.adId))
      .process(new FilterBlackListProcessFunction(100))


    val resultStream: DataStream[CountByProvince] = filterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new CountAdsClickAgg(), new CountResultByProvince())

    resultStream.print("filter by blackList")
    filterBlackListStream.getSideOutput(blackListOutputTag).printToErr("blackList")

    env.execute("ads click statistics by black list")
  }

  /**
    * 将超过最大次数的点击输出到侧输出流
    *
    * @param maxSize
    */
  class FilterBlackListProcessFunction(maxSize: Long) extends KeyedProcessFunction[(Long,Long), AdClickLog, AdClickLog] {

    var listAdClickLog: ListState[AdClickLog] = _
    var isSent:ValueState[Boolean] = _
    var isResetTimer:ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      listAdClickLog = getRuntimeContext.getListState(new ListStateDescriptor[AdClickLog]("listAdClick-state", classOf[AdClickLog]))
      isSent = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent-state",classOf[Boolean]))
      isResetTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("isResetTimer-state",classOf[Long]))
    }


    override def processElement(value: AdClickLog,
                                ctx: KeyedProcessFunction[(Long,Long), AdClickLog, AdClickLog]#Context,
                                out: Collector[AdClickLog]): Unit = {
      import scala.collection.JavaConversions._
      val adClickLogs: lang.Iterable[AdClickLog] = listAdClickLog.get()
      if(adClickLogs.isEmpty){
        isSent.update(true)
        val timestamp: Long = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * 24 * 60 * 60* 1000
        ctx.timerService().registerProcessingTimeTimer(timestamp )
        isResetTimer.update(timestamp)
      }
      if (maxSize < adClickLogs.size + 1  && isSent.value() ) {
        isSent.update(false)
        //输出到侧输出流
        ctx.output(blackListOutputTag,
          BlackListWarning(value.userId,value.adId,s"user  ${value.userId} click ads over ${maxSize} times"))
      } else if(maxSize >= adClickLogs.size + 1 ){
        //更新listState
        listAdClickLog.add(value)
        out.collect(value)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext,
                         out: Collector[AdClickLog]): Unit = {
      if(isResetTimer.value() == timestamp){
        listAdClickLog.clear()
        isSent.clear()
      }
    }
  }

}

