package com.getcake.aggregation.trigger

import com.getcake.sourcetype.StreamData
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class OneSecondIntervalTrigger extends Trigger[StreamData, TimeWindow]
{
  override def onElement(streamData: StreamData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // firstSeen will be false if not set yet
    val alertUseMapper: MapState[Tuple3[Int, Int, Int], Boolean] = ctx.getPartitionedState(new MapStateDescriptor[Tuple3[Int, Int, Int], Boolean]("alertUseMapper", classOf[Tuple3[Int, Int, Int]], classOf[Boolean]))

    // register initial timer only for first element
    if (true) {
      // compute time for next early firing by rounding watermark to second
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      ctx.registerEventTimeTimer(t)
      // register timer for the window end
      ctx.registerEventTimeTimer(window.getEnd)
    }
    // Continue. Do not evaluate per element
    TriggerResult.CONTINUE
  }

  override def onEventTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (timestamp == window.getEnd) {
      // final evaluation and purge window state
      TriggerResult.FIRE_AND_PURGE
    } else {
      // register next early firing timer
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      if (t < window.getEnd) {
        ctx.registerEventTimeTimer(t)
      }
      // fire trigger to evaluate window
      TriggerResult.FIRE
    }
  }

  override def onProcessingTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // Continue. We don't use processing time timers
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // clear trigger state

  }
}
