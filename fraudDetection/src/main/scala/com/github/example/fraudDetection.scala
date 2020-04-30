package com.github.example

import java.util.Properties

import com.github.example.filters.{alarmedCustomerCheck, cityChangeCheck, excessiveTransactionCheck, lostCardsCheck}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

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
                                .flatMap(new excessiveTransactionCheck())

  val changedCity: DataStream[(String, String)] = streamedData
                                                      .keyBy(value => value._1)
                                                      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                                                      .process(new cityChangeCheck())


  val allFlaggedTransactions = alarmedCustTransaction.union(lostCardTransaction, excessiveTransactions, changedCity)

  try {
    allFlaggedTransactions.writeAsText("../analisysResult/FlaggedTransactions")
  }
  catch {
    case e: Exception => println(s"Message: ${e.getMessage}, Cause: ${e.getCause}")
  }

  env.execute()
}




