package com.github.example.filters

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class cityChangeCheck extends ProcessWindowFunction[(String, String), (String,String), String, TimeWindow] {
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