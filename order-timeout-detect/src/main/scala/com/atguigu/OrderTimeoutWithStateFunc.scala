package com.atguigu

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OrderTimeoutWithStateFunc {

  val outputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")
  val lateTime: Long = 15 * 60 * 1000

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    val orderFilePath: String = getClass.getResource("/OrderLog.csv").getPath
    //val orderDataStream: DataStream[OrderResult] = env.readTextFile(orderFilePath)
    val dataStream: DataStream[OrderResult] = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val splits: Array[String] = data.split(",")
        OrderEvent(splits(0).toLong, splits(1), splits(2), splits(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)
      .process(new OrderTimeoutProccessFunc())

    dataStream.print("pay")
    dataStream.getSideOutput(outputTag).print("timeout")

    env.execute("order timeout with state func job")
  }

  class OrderTimeoutProccessFunc() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

    lazy val createState: ValueState[Boolean] =
      getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("create-state", classOf[Boolean]))
    lazy val payState: ValueState[Boolean] =
      getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("pay-state", classOf[Boolean]))
    lazy val payRegisterTimer: ValueState[Long] =
      getRuntimeContext.getState(new ValueStateDescriptor[Long]("payRegister-state", classOf[Long]))
    lazy val createRegisterTimer: ValueState[Long] =
      getRuntimeContext.getState(new ValueStateDescriptor[Long]("createRegister-state", classOf[Long]))

    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                out: Collector[OrderResult]): Unit = {
      val isCreated: Boolean = createState.value()
      val createTimer: Long = createRegisterTimer.value()
      val isPay: Boolean = payState.value()
      val payTimer: Long = payRegisterTimer.value()
      if (value.eventType == "create") {
        //已经支付
        if (isPay) {
          val times: Long = payTimer - lateTime - value.eventTime * 1000
          if (times >= 0 && times < lateTime) {
            //未超时支付
            out.collect(OrderResult(value.orderId, "pay successfully1"))
          } else {
            //超时支付
            ctx.output(outputTag, OrderResult(value.orderId, "pay but over timeout1"))
          }
          clear()
          ctx.timerService().deleteEventTimeTimer(payTimer)
        } else {
          //未支付
          //注册定时器,更新状态
          createState.update(true)
          val ts: Long = value.eventTime * 1000 + lateTime
          createRegisterTimer.update(ts)
          ctx.timerService().registerEventTimeTimer(ts)
        }
      }
      if (value.eventType == "pay") {
        if (isCreated) {
          val times: Long = value.eventTime * 1000 - createTimer + lateTime
          if (times >= 0 && times < lateTime) {
            //未超时支付
            out.collect(OrderResult(value.orderId, "pay successfully2"))
          } else {
            //超时支付
            ctx.output(outputTag, OrderResult(value.orderId, "pay but over timeout2"))
          }
          clear()
          ctx.timerService().deleteEventTimeTimer(createTimer)
        } else {
          //注册定时器,更新状态
          payState.update(true)
          val ts: Long = value.eventTime * 1000 + lateTime
          payRegisterTimer.update(ts)
          ctx.timerService().registerEventTimeTimer(ts)
        }
      }

    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                         out: Collector[OrderResult]): Unit = {
      val isCreated: Boolean = createState.value()
      val isPay: Boolean = payState.value()
      if (isCreated && isPay) {
        ctx.output(outputTag, OrderResult(ctx.getCurrentKey, "pay but time out3"))
      } else {
        ctx.output(outputTag, OrderResult(ctx.getCurrentKey, "pay time out4"))
      }
      clear()
    }

    def clear(): Unit = {
      createState.clear()
      createRegisterTimer.clear()
      payState.clear()
      payRegisterTimer.clear()
    }
  }

}

