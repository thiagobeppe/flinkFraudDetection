package com.github.example

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

case class alarmedCustumer(id: String, account: String)
case class lostCards(id: String, timeStamp: String, name: String, status: String)

object fraudDetection extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")

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
                      .addSource(new FlinkKafkaConsumer[String]("TRANSACTIONS_TOPIC", new SimpleStringSchema(), properties))
                      .map(value => (value.split(",")(3), value))

  val alarmedCustTransaction: DataStream[(String, String)] = streamedData
                                .keyBy(0)
                                .connect(alarmedCostumerBroadCast).process(new alarmedCustomerCheck())

  val lostCardTransaction: DataStream[(String, String)] = streamedData
                                .keyBy(0)
                                .connect(lostCardsBroadCast).process(new lostCardsCheck())

  val excessiveTransactions: DataStream[(String, String)] = streamedData
                                .map( value => (value._1,value._2, 1))
                                .keyBy(0)
                                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                                .sum(2)
                                .flatMap((value, out: Collector[(String, String)]) => {
                                  if(value._3 >10){
                                    out.collect(("___Alarm____", s"${value} marked MORE THEN 10 times"))
                                  }
                                })

  val changedCity: DataStream[(String, String)] = streamedData
                                                      .keyBy(value => value._1)
                                                      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                                                      .process(new cityChange())

  alarmedCustTransaction.print()
  lostCardTransaction.print()
  excessiveTransactions.print()
  changedCity.print()

  env.execute()
}

class alarmedCustomerCheck extends KeyedBroadcastProcessFunction[String, (String,String), alarmedCustumer, (String, String)]{
  override def processElement(value: (String, String),
                              ctx: KeyedBroadcastProcessFunction[String, (String, String), alarmedCustumer, (String, String)]#ReadOnlyContext,
                              out: Collector[(String, String)]): Unit = {
    ctx.getBroadcastState(new MapStateDescriptor("lost_cards", classOf[String], classOf[alarmedCustumer])).immutableEntries().forEach(
      {entry => {
        val alarmedCustId = entry.getKey
        val transactionId = value._1

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

class lostCardsCheck extends KeyedBroadcastProcessFunction[String, (String,String), lostCards, (String,String)]{
  override def processElement(value: (String, String),
                              ctx: KeyedBroadcastProcessFunction[String, (String, String), lostCards, (String, String)]#ReadOnlyContext,
                              out: Collector[(String, String)]): Unit = {
    ctx.getBroadcastState(new MapStateDescriptor("lost_cards", classOf[String], classOf[lostCards])).immutableEntries().forEach(
      { entry => {
        val entryNumber = entry.getKey
        val credidCardNumber = value._2.split(",")(5)
        if(entryNumber == credidCardNumber){
          out.collect(("___Alarm____", s"Transaction: ${value} is by an LOST card"))
        }
       }
      })

  }

  override def processBroadcastElement(value: lostCards,
                                       ctx: KeyedBroadcastProcessFunction[String, (String, String), lostCards, (String, String)]#Context,
                                       out: Collector[(String, String)]): Unit = {
    ctx.getBroadcastState(new MapStateDescriptor("lost_cards", classOf[String], classOf[lostCards])).put(value.id, value)
  }
}

class cityChange extends ProcessWindowFunction[(String, String), (String,String), String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, String)],
                       out: Collector[(String, String)]): Unit = {
    var lastCity = ""
    var changeCount = 0
    elements.foreach( value => {
      val city = value._2.split(",")(2).toLowerCase
      if(lastCity.isEmpty){
        lastCity = city
      }
      else {
        if(!city.equals(lastCity)) {
          lastCity = city
          changeCount += 1
        }
      }
      if(changeCount > 2){
        out.collect(("___Alarm____", s"${value} marked for FREQUENT city changes"))
      }

    })
  }
}