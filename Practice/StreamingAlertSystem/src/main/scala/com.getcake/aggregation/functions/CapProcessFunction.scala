package com.getcake.aggregation.functions

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import com.getcake.StreamingAlertSystem

class CapProcessFunction extends ProcessWindowFunction[(String, Int, Int, Int, Long, Long, Int, Long), Map[(Int, Int, Int), Int], Int, TimeWindow] {

  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  override def process(key: Int, ctx: Context, elements: Iterable[(String, Int, Int, Int, Long, Long, Int, Long)], out: Collector[Map[(Int, Int, Int), Int]]): Unit = {
    // count readings

    println("CapProcessFunction")
    elements.map( x => {println("element window: ", x)
      0
    })
    val mapCount = elements.groupBy(e => (e._2, e._3, e._4)).map(x => {(x._1, x._2.count(_ => true))})
    // get current watermark
    val evalTime = ctx.currentWatermark
    val processTime = ctx.currentProcessingTime

    val last = elements.last
    val capCurentValeState = ctx.windowState.getState(new ValueStateDescriptor[Int]("cur_" + (last._2, last._3, last._4).toString(), createTypeInformation[Int]))

    val curTimeInstance = Calendar.getInstance
    val now = curTimeInstance.getTime
    println()
    println("processTime:", timeformater.format(processTime), "watermark: ",timeformater.format(ctx.currentWatermark))
    println("process delay: ", processTime - last._6, "waterDelay:",  ctx.currentWatermark - last._6)
    println("curTime:",now , timeformater.format(now))
    // emit result
    print("CustomProcessFunction process ")

    println("cur:", capCurentValeState.value())
    println()
    capCurentValeState.clear()

    mapCount.foreach(a => {
        ctx.output(StreamingAlertSystem.entityCapStatuses, (a._1._1, a._1._2, a._1._3, capCurentValeState.value()))
    })
    out.collect(mapCount)
  }
}
