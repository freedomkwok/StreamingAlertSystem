package com.ngenda.aggregation.triggers

import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class OneSecondIntervalTrigger extends Trigger[(String, Int, Int, Int, Long, Long, Int, Long), TimeWindow]
{
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  override def onElement(filteredStreamData: (String, Int, Int, Int, Long, Long, Int, Long), timestamp: Long, flinkwindow: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val d_filteredStream :(String, Int, Int, Int, Long, Long, Int, Long) = filteredStreamData.asInstanceOf[(String, Int, Int, Int, Long, Long, Int, Long)]

    //val entityKey = d_filteredStream._2 + "_" + d_filteredStream._3 + "_" + d_filteredStream._4
    val watermark = ctx.getCurrentWatermark()
    val processTime = ctx.getCurrentProcessingTime()

    val alertUseKey : (Int, Int, Int) = (d_filteredStream._2, d_filteredStream._3, d_filteredStream._4)
//    val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
//    println("firstItem calculated watermark: ", t, timeformater.format(t), "actual watermark", watermark, timeformater.format(watermark), "process", processTime, timeformater.format(processTime))
//    println("windowStart: ", timeformater.format(flinkwindow.getStart) ,"windowEnd: ", timeformater.format(flinkwindow.getEnd))
     println("on element")

    val alertUse: ValueState[Int] = ctx.getPartitionedState(
      new ValueStateDescriptor[Int](alertUseKey.toString, createTypeInformation[Int]))

    alertUse.update(alertUse.value() + 1)


    TriggerResult.CONTINUE
  }

  override def onEventTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (timestamp == window.getEnd) {
      println("OneSecondIntervalTrigger onEventTime FireAndPurge", "timestamp: ", timeformater.format(timestamp), "windoend ", timeformater.format(window.getEnd))
      // final evaluation and purge window state
      TriggerResult.FIRE_AND_PURGE
    } else {
      // register next early firing timer
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))

      println("onEventTime Fire watermark: ", timeformater.format(t), t, t < window.getEnd )
      if (t < window.getEnd) {
        ctx.registerEventTimeTimer(t)

      }
      // fire trigger to evaluate window
      TriggerResult.FIRE
    }
  }

  override def onProcessingTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // Continue. We don't use processing time timers
    println("onProcessingTime")
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // clear trigger state
    println("clear")
  }
}


