package com.github.example.filters

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class excessiveTransactionCheck extends FlatMapFunction[(String,String,Int), (String,String)]{
  override def flatMap(value: (String, String, Int), out: Collector[(String, String)]): Unit = {
      if(value._3 >10){
        out.collect(("___Alarm____", s"${value} marked MORE THEN 10 times"))
      }
  }
}
