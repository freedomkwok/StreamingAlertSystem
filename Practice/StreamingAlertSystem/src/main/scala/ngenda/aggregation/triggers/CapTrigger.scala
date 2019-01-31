package com.ngenda.aggregation.triggers

import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class CapTrigger extends Trigger[(String, Int, Int, Int, Long, Long, Int, Long), TimeWindow]{
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  override def onElement(filteredStreamData: (String, Int, Int, Int, Long, Long, Int, Long), timestamp: Long, flinkwindow: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

    val entityKey = (filteredStreamData._2, filteredStreamData._3, filteredStreamData._4)
    val watermark = ctx.getCurrentWatermark()
    val processTime = ctx.getCurrentProcessingTime()

    //    val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
    //    println("firstItem calculated watermark: ", t, timeformater.format(t), "actual watermark", watermark, timeformater.format(watermark), "process", processTime, timeformater.format(processTime))
    //    println("windowStart: ", timeformater.format(flinkwindow.getStart) ,"windowEnd: ", timeformater.format(flinkwindow.getEnd))
    //    println()

    val capCurentValeState: ValueState[Int] = ctx.getPartitionedState(
      new ValueStateDescriptor[Int]("cur_" + entityKey.toString, createTypeInformation[Int]))

    val capTargetValeState: ValueState[Int] = ctx.getPartitionedState(
      new ValueStateDescriptor[Int]("tgt_" + entityKey.toString, createTypeInformation[Int]))


    capCurentValeState.update(capCurentValeState.value() + 1)


    if(filteredStreamData._7 <= capCurentValeState.value()) {
      println("FIRE_AND_PURGE")
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      ctx.registerEventTimeTimer(t)
      TriggerResult.FIRE_AND_PURGE
    }
    else{
      println("<===OnElement===>", entityKey.toString, " ", capCurentValeState.value(), filteredStreamData._8, timeformater.format(filteredStreamData._8), filteredStreamData._8 - ctx.getCurrentWatermark, filteredStreamData._8 - timestamp)
      println()
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("onEventTime")
//    if (timestamp == window.getEnd) {
//      println("OneSecondIntervalTrigger onEventTime FireAndPurge", "timestamp: ", timeformater.format(timestamp), "windoend ", timeformater.format(window.getEnd))
//      // final evaluation and purge window state
//      TriggerResult.FIRE_AND_PURGE
//    } else {
//      // register next early firing timer
//      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
//
//      println("onEventTime Fire watermark: ", timeformater.format(t), t, t < window.getEnd )
//      if (t < window.getEnd) {
//        ctx.registerEventTimeTimer(t)
//
//      }
//      // fire trigger to evaluate window
//      TriggerResult.FIRE
//    }
    TriggerResult.CONTINUE
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
