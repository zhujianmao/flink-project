package com.atguigu

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


case class OrderEvent(orderId: Long, eventType: String, tx: String, eventTime: Long)

case class OrderResult(orderId: Long, msg: String)


/**
  * 检测九分钟内订单失效
  */
object OrderTimeoutWithCEP {

  val outputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val filePath: String = getClass.getResource("/OrderLog.csv").getPath
    val dataStream: KeyedStream[OrderEvent, Long] = env.readTextFile(filePath)
      .map(data => {
        val splits: Array[String] = data.split(",")
        OrderEvent(splits(0).toLong, splits(1), splits(2), splits(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    val orderTimeOutPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay").where(!_.tx.isEmpty)
      .within(Time.minutes(9))

    val timeOutPatternStream: PatternStream[OrderEvent] = CEP.pattern(dataStream, orderTimeOutPattern)

    val resultStream: DataStream[OrderResult] = timeOutPatternStream.select(outputTag,new PayTimeout(), new PaySuccess())

    resultStream.print("pay")
    resultStream.getSideOutput(outputTag).printToErr("timeout")

    env.execute(" order timeout with CEP")
  }

}

class PaySuccess() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val event: OrderEvent = map.get("follow").iterator().next()
    OrderResult(event.orderId,"pay successfully")
  }
}


class PayTimeout() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], time: Long): OrderResult = {
    val orderId: Long = map.get("begin").iterator().next().orderId
    OrderResult(orderId,"time out" )
  }
}
