package com.getcake.aggregation.functions

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class CustomProcessFunction extends ProcessWindowFunction[(String, Int, Int, Int, Int), Int, String, TimeWindow] {
  override def process(key: String, ctx: Context, elements: Iterable[(String, Int, Int, Int, Int)], out: Collector[Int]): Unit = {
    // count readings
    val cnt = elements.count(_ => true)
    // get current watermark
    val evalTime = ctx.currentWatermark

    // emit result
    out.collect(10)
  }
}
