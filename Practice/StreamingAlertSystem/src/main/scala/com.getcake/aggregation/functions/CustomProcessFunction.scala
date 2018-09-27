package com.getcake.aggregation.functions

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class CustomProcessFunction extends ProcessWindowFunction[(String, Int, Int, Int, Long, Long), Map[Int, Int], Int, TimeWindow] {
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
  override def process(key: Int, ctx: Context, elements: Iterable[(String, Int, Int, Int, Long, Long)], out: Collector[Map[Int, Int]]): Unit = {
    // count readings
    val totalCount = elements.groupBy(_._3).map(x => {(x._1, x._2.count(_ => true))})
    // get current watermark
    val evalTime = ctx.currentWatermark
    val processTime = ctx.currentProcessingTime

    val last = elements.last
    println("endtime", timeformater.format(last._6), timeformater.format(processTime), "delay: ", processTime - last._6 , "watermark: ",timeformater.format(ctx.currentWatermark))

    // emit result
    print("CustomProcessFunction process ")
    out.collect(totalCount)
  }
}
