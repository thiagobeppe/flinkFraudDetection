package com.github.example.filters

import com.github.example.lostCards
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

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
