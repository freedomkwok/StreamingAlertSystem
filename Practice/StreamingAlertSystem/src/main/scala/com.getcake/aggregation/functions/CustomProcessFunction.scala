package com.getcake.aggregation.functions

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class CustomProcessFunction extends ProcessWindowFunction[(String, Int, Int, Int, Long, Long), Int, Int, TimeWindow] {
  override def process(key: Int, ctx: Context, elements: Iterable[(String, Int, Int, Int, Long, Long)], out: Collector[Int]): Unit = {
    // count readings
    val totalCount = elements.count(_ => true)
    // get current watermark
    val evalTime = ctx.currentWatermark

    // emit result
    print("CustomProcessFunction process ")
    out.collect(totalCount)
  }
}
