package com.github.example

import org.apache.flink.streaming.api.scala._

case class alarmedCostumer(id: String, account: String)
case class lostCards(id: String, timeStamp: String, name: String, status: String)

object fraudDetection extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val alarmedDataSet = env.readTextFile("../datasets/alarmed_cust.txt").map(_.split(",")).map(value => {
    new alarmedCostumer(value(0),value(1))
  })
  val lostCardsDataSet = env.readTextFile("../datasets/lost_cards.txt").map(_.split(",")).map(value => {
    new lostCards(value(0),value(1),value(2),value(3))
  })

  alarmedDataSet.print()
  lostCardsDataSet.print()

  env.execute()
}
