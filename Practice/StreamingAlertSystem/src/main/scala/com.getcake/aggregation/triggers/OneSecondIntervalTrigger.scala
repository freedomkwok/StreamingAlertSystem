package com.getcake.aggregation.triggers

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class OneSecondIntervalTrigger extends Trigger[(String, Int, Int, Int, Long, Long), TimeWindow]
{
  override def onElement(filteredStreamData: (String, Int, Int, Int, Long, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

    println("OneSecondIntervalTrigger onElement")
    // firstSeen will be false if not set yet
//    val alertUseMapper: MapState[(Int, Int, Int), Boolean] = ctx.getPartitionedState(new MapStateDescriptor[(Int, Int, Int), Boolean]("alertUseMapper", classOf[(Int, Int, Int)], classOf[Boolean]))
//    val publisherKey : (Int, Int, Int) = (filteredStreamData._2, filteredStreamData._3, 1)
//    val offerKey : (Int, Int, Int) = (filteredStreamData._2, filteredStreamData._4, 2)
//    val campaignKey : (Int, Int, Int) = (filteredStreamData._2, filteredStreamData._5, 3)

    // check if we may forward the reading
    val t = ctx.getCurrentWatermark + (10000 - (ctx.getCurrentWatermark % 10000))
    ctx.registerEventTimeTimer(t)
    // register timer for the window end
    ctx.registerEventTimeTimer(window.getEnd)
    // register initial timer only for first element
//    if(alertUseMapper.contains(publisherKey) || alertUseMapper.contains(offerKey) || alertUseMapper.contains(campaignKey))  {
//      // compute time for next early firing by rounding watermark to second
//
//    }
    // Continue. Do not evaluate per element
    TriggerResult.CONTINUE
  }

  override def onEventTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

    println("OneSecondIntervalTrigger onEventTime")
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
    println("OneSecondIntervalTrigger onProcessingTime")
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // clear trigger state
    println("OneSecondIntervalTrigger clear")
  }
}
