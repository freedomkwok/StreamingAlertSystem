package com.ngenda.aggregation.windowassigners

import java.text.SimpleDateFormat
import java.util.Collections

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows._

class MiniBatchIntervalWindowAssigner(interval: Int) extends WindowAssigner[Object, TimeWindow] {

  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  override def assignWindows(filteredStream: Object, ts: Long, ctx: WindowAssigner.WindowAssignerContext): java.util.List[TimeWindow] = {

    val d_filteredStream :(String, Int, Int, Int, Long, Long, Int, Long) = filteredStream.asInstanceOf[(String, Int, Int, Int, Long, Long, Int, Long)]

    val startTime = d_filteredStream._5 - (d_filteredStream._5 % 1000)
    val endTime = d_filteredStream._6 - (d_filteredStream._6 % 1000)

    if(d_filteredStream._5 > ts) {
      println("warning arrive early")
      null
    }
    else if(d_filteredStream._6 < ts) {
      println("warning arrive late")
      null
    }
    else {
      //print("_ ")
     // println("assignWindows ontime:" , d_filteredStream._2, d_filteredStream._3, timeformater.format(ts))
      Collections.singletonList(new TimeWindow(startTime, endTime))
    }

    // emitting the corresponding time window
    //println("CustomWindowAssigner assignWindows", timeformater.format(startTime) , timeformater.format(endTime))

    //Collections.singletonList(new TimeWindow(d_filteredStream._5, d_filteredStream._6))
  }

  override def getDefaultTrigger(env: environment.StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    println("CustomWindowAssigner getDefaultTrigger")
    EventTimeTrigger.create()
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
    println("CustomWindowAssigner getWindowSerializer")
    new TimeWindow.Serializer
  }

  override def isEventTime = true
}
