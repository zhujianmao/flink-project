package com.atguigu

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，
  */
object LoginFailDetectByCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val dataStream: DataStream[LoginEvent] = env.readTextFile(getClass.getResource("/LoginLog.csv").getPath)
    val dataStream: DataStream[LoginEvent] = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong * 1000)

      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      })

    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("start").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    val loginFailCEP: PatternStream[LoginEvent] = CEP.pattern(dataStream, pattern)

    val loginFailStream: DataStream[LoginFailResult] = loginFailCEP.select[LoginFailResult](new LoginFailPatternFunction())


    loginFailStream.print("loginFail")

    env.execute("login fail detect job by CEP")
  }

}

class LoginFailPatternFunction() extends PatternSelectFunction[LoginEvent, LoginFailResult] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailResult = {
    val start: LoginEvent = map.get("start").iterator().next()
    val end: LoginEvent = map.get("next").iterator().next()
    LoginFailResult(start.userId, start.eventTime, end.eventTime, "login fail 2 times in 2 seconds")
  }
}

