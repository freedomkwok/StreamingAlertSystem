package com.getcake.aggregation.function

import com.getcake.sourcetype.StreamData
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class CustomProcessFunction extends ProcessWindowFunction[StreamData, Int, Int, TimeWindow] {
  override def process(key: Int, ctx: Context, elements: Iterable[StreamData], out: Collector[Int]): Unit = {
    // count readings
    val cnt = elements.count(_ => true)
    // get current watermark
    val evalTime = ctx.currentWatermark

    // emit result
    out.collect(10)
  }
}
