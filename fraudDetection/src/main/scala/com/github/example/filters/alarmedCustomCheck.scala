package com.github.example.filters

import com.github.example.alarmedCustumer
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

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
