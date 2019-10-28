package com.atguigu

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

case class ReceiptResult(txId: String, msg: String)

/**
  *   利用coProcessFunction实现两条流实时对账
  *   账目正确的输出到主流,不符合的输出到侧输出流中
  *
  */
object TxMatchWithConnect {

  val outputTagWithOrder: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTag")
  val outputTagWithReceipt: OutputTag[ReceiptResult] = new OutputTag[ReceiptResult]("receiptTag")
  val lateTime: Long = 60*1000L

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderDataStream: KeyedStream[OrderEvent, String] = env.readTextFile(getFilePath("OrderLog.csv"))
      .map(data => {
        val splits: Array[String] = data.split(",")
        OrderEvent(splits(0).toLong, splits(1), splits(2), splits(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.tx)

    val receiptStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(getFilePath("ReceiptLog.csv"))
      .map(data => {
        val splits: Array[String] = data.split(",")
        ReceiptEvent(splits(0), splits(1), splits(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)

    val resultStream: DataStream[OrderResult] = orderDataStream.connect(receiptStream)
      .process(new CheckAccount())

    resultStream.print("order and receipt")
    resultStream.getSideOutput(outputTagWithReceipt).print("receipt")
    resultStream.getSideOutput(outputTagWithOrder).print("order")

  env.execute("tx match with connect job")
  }

  class CheckAccount() extends CoProcessFunction[OrderEvent,ReceiptEvent,OrderResult]{

    lazy val orderState: ValueState[OrderEvent] =
      getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order-state",classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] =
      getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent]))
    lazy val timer:ValueState[Long] =
      getRuntimeContext.getState(new ValueStateDescriptor[Long]("order-timer",classOf[Long]))

    override def processElement1(value: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, OrderResult]#Context,
                                 out: Collector[OrderResult]): Unit = {
      val receiptEvent: ReceiptEvent = receiptState.value()
      val ts: Long = value.eventTime * 1000 + lateTime
      if(receiptEvent != null ){
        if(receiptEvent.txId == value.tx){
          out.collect(OrderResult(value.orderId,"success"))
        }else{
          ctx.output(outputTagWithOrder,OrderResult(value.orderId,s"txId ${value.tx} in order  is not the same in receipt ${receiptEvent.txId}"))
        }
        ctx.timerService().deleteEventTimeTimer(ts)
        clear()
      }else {
        //注册定时器和更新状态
        timer.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
        orderState.update(value)
      }
    }

    override def processElement2(value: ReceiptEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, OrderResult]#Context,
                                 out: Collector[OrderResult]): Unit = {
      val orderEvent: OrderEvent = orderState.value()
      val ts: Long = value.eventTime * 1000 + lateTime
      if(orderEvent != null ){
        if(orderEvent.tx == value.txId){
          out.collect(OrderResult(orderEvent.orderId,"success"))
        }else{
          ctx.output(outputTagWithReceipt,ReceiptResult(value.txId,s"txId ${value.txId} in receipt  is not the same in order ${orderEvent.tx}"))
        }
        ctx.timerService().deleteEventTimeTimer(ts)
        clear()
      }else {
        //注册定时器和更新状态
        timer.update(ts)
        ctx.timerService().registerEventTimeTimer(ts)
        receiptState.update(value)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, ReceiptEvent, OrderResult]#OnTimerContext,
                         out: Collector[OrderResult]): Unit = {
      if(timer.value() == timestamp){
        val orderEvent: OrderEvent = orderState.value()
        val receiptEvent: ReceiptEvent = receiptState.value()
        if(orderEvent != null){
          ctx.output(outputTagWithOrder,OrderResult(orderEvent.orderId,"do not exists receipt with order"))
        }
        if(receiptEvent != null){
          ctx.output(outputTagWithReceipt,ReceiptResult(receiptEvent.txId,"do not exists order with receipt"))
        }
      }
      clear()
    }

    def clear(): Unit ={
      orderState.clear()
      timer.clear()
      receiptState.clear()
      timer.clear()
    }
  }


  def getFilePath(fileName:String): String ={
    getClass.getResource("/"+fileName).getPath
  }
}


