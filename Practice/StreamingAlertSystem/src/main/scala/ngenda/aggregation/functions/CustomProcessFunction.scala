package com.ngenda.aggregation.functions

import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import com.ngenda.StreamingAlertSystem

class CustomProcessFunction extends ProcessWindowFunction[(String, Int, Int, Int, Long, Long, Int, Long), Map[(Int, Int, Int), Int], Int, TimeWindow] {

  lazy val alertUseMapper: MapState[(Int, Int, Int), (Boolean, Long, Long, Int)] =
    getRuntimeContext.getMapState(
      new MapStateDescriptor[(Int, Int, Int), (Boolean, Long, Long, Int)]("alertUseMapper", createTypeInformation[(Int, Int, Int)], createTypeInformation[(Boolean, Long, Long, Int)])
    )
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  override def process(key: Int, ctx: Context, elements: Iterable[(String, Int, Int, Int, Long, Long, Int, Long)], out: Collector[Map[(Int, Int, Int), Int]]): Unit = {
    // count readings
    elements.map( x => {println("element window: ", x)
    0
    })
    val mapCount = elements.groupBy(e => (e._2, e._3, e._4)).map(x => {(x._1, x._2.count(_ => true))})
    // get current watermark
    val evalTime = ctx.currentWatermark
    val processTime = ctx.currentProcessingTime

    val last = elements.last
    println("scheduled engtime:", timeformater.format(last._6))
    println("processTime:", timeformater.format(processTime), "watermark: ",timeformater.format(ctx.currentWatermark))
    println("process delay: ", processTime - last._6, "waterDelay:",  ctx.currentWatermark - last._6)
    // emit result
    print("CustomProcessFunction process ")
    mapCount.foreach(a => {
      if(alertUseMapper.contains(a._1)) {
         val oldValue = alertUseMapper.get(a._1)
        val newTotal = oldValue._4 + a._2
        alertUseMapper.put(a._1, (oldValue._1, oldValue._2, oldValue._3, newTotal))
        ctx.output(StreamingAlertSystem.entityCapStatuses, (a._1._1, a._1._2, a._1._3, newTotal))
      }
    })
    out.collect(mapCount)
  }
}
