package com.atguigu


import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class LoginFailResult(userId: Long, startTime: Long, endTime: Long, msg: String)

/**
  * 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，
  */
object LoginFailDetect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env.readTextFile(getClass.getResource("/LoginLog.csv").getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong * 1000)

      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      })

    val loginFailStream: DataStream[LoginFailResult] = dataStream.keyBy(_.userId)
      .process(new LoginFailProcessFunction(2))

    loginFailStream.print("loginFail")

    env.execute("login fail detect job")
  }

}

import scala.collection.JavaConversions._

class LoginFailProcessFunction(maxTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailResult] {

  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfaillist-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailResult]#Context,
                              out: Collector[LoginFailResult]): Unit = {
    if (value.eventType == "fail") {
      loginFailListState.add(value)
      ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 2000)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailResult]#OnTimerContext,
                       out: Collector[LoginFailResult]): Unit = {
    if (loginFailListState.get().size >= maxTimes) {
      val loginEvents: List[LoginEvent] = loginFailListState.get().toList.sortBy(_.eventTime)
      out.collect(LoginFailResult(ctx.getCurrentKey,
        loginEvents.head.eventTime,
        loginEvents.last.eventTime,
        s"login fail ${loginFailListState.get().size} times in 2s"))
    }
  }
}