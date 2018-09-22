package com.getcake.aggregation.windowassigners

import java.util.Collections

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.windows._

/** A custom window that groups events into 30 second tumbling windows. */
class CustomWindowAssigner() extends WindowAssigner[Object, TimeWindow]
{
  override def assignWindows(filteredStream: Object, ts: Long, ctx: WindowAssigner.WindowAssignerContext): java.util.List[TimeWindow] = {
    println("CustomWindowAssigner assignWindows")

    val d_filteredStream :(String, Int, Int, Int, Long, Long) = filteredStream.asInstanceOf[(String, Int, Int, Int, Long, Long)]
    val startTime = ts - (ts % 10000)
    val endTime = startTime + 10000
    // emitting the corresponding time window
    Collections.singletonList(new TimeWindow(startTime, endTime))
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
