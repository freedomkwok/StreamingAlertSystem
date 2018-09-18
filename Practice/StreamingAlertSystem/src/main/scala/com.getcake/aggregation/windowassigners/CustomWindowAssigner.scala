package com.getcake.aggregation.windowassigners

import java.util.Collections

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import com.getcake.aggregation.windows.{CustomWindow}

/** A custom window that groups events into 30 second tumbling windows. */
class CustomWindowAssigner() extends WindowAssigner[Object, TimeWindow]
{
  override def assignWindows(o: Object, ts: Long, ctx: WindowAssigner.WindowAssignerContext): java.util.List[TimeWindow] = {

    val startTime = ts - (ts % 1000)
    val endTime = startTime + 1000
    // emitting the corresponding time window
    Collections.singletonList(new TimeWindow(startTime, endTime))
  }

  override def getDefaultTrigger(env: environment.StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    EventTimeTrigger.create()
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }

  override def isEventTime = true
}
