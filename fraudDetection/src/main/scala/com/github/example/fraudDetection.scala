package com.github.example

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class alarmedCustumer(id: String, account: String)
case class lostCards(id: String, timeStamp: String, name: String, status: String)

object fraudDetection extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val alarmedCostumerBroadCast: BroadcastStream[alarmedCustumer] = env
                                                                  .readTextFile("../datasets/alarmed_cust.txt")
                                                                  .map(_.split(","))
                                                                  .map(value => {new alarmedCustumer(value(0),value(1))})
                                                                  .broadcast(new MapStateDescriptor("lost_cards", classOf[String], classOf[alarmedCustumer]))
  val lostCardsBroadCast: BroadcastStream[lostCards] = env
                                                        .readTextFile("../datasets/lost_cards.txt")
                                                        .map(_.split(","))
                                                        .map(value => {new lostCards(value(0),value(1),value(2),value(3))})
                                                        .broadcast(new MapStateDescriptor("lost_cards", classOf[String], classOf[lostCards]))

  val streamedData: DataStream[(String, String)] = env
                      .readTextFile("../datasets/bank_data.txt")
                      .map(value => (value.split(",")(3), value))

  val alarmedCustTransaction = streamedData
                                .keyBy(0)
                                .connect(alarmedCostumerBroadCast).process(new alarmedCustomerCheck())


  env.execute()
}

class alarmedCustomerCheck extends KeyedBroadcastProcessFunction[String, (String,String), alarmedCustumer, (String, String)]{
  override def processElement(value: (String, String),
                              ctx: KeyedBroadcastProcessFunction[String, (String, String), alarmedCustumer, (String, String)]#ReadOnlyContext,
                              out: Collector[(String, String)]): Unit = {
    ctx.getBroadcastState(new MapStateDescriptor("lost_cards", classOf[String], classOf[alarmedCustumer])).immutableEntries().forEach(
      {entry => {
        val alarmedCustId = entry.getKey
        val transactionId = value._2.split(",")(3)

        if(transactionId == alarmedCustId){
          out.collect(("___Alarm____", s"Transaction: ${value} is by an ALARMED customer"))
        }
      }}
    )

  }

  override def processBroadcastElement(value: alarmedCustumer,
                                       ctx: KeyedBroadcastProcessFunction[String, (String, String), alarmedCustumer, (String, String)]#Context,
                                       out: Collector[(String, String)]): Unit = {
      ctx.getBroadcastState(new MapStateDescriptor("lost_cards", classOf[String], classOf[alarmedCustumer])).put(value.id, value)
  }
}