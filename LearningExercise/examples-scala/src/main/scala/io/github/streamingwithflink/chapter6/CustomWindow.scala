package io.github.streamingwithflink.chapter6

import java.text.SimpleDateFormat
import java.util.Collections

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object CustomWindows {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    //sensorData.print()

    val countsPerThirtySecs = sensorData
      .keyBy(_.id)
      // a custom window assigner for 30 second tumbling windows
      .window(new ThirtySecondsWindows(10000))
      // a custom trigger that fires early (at most) every second
      .trigger(new OneSecondIntervalTrigger)
      // count readings per window
      .process(new CountFunction)

    //countsPerThirtySecs.print()

    env.execute()
  }
}

/** A custom window that groups events into 30 second tumbling windows. */
class ThirtySecondsWindows(windowPeriod: Long)
    extends WindowAssigner[Object, TimeWindow] {
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
  val windowSize: Long = windowPeriod

  override def assignWindows(
      o: Object,
      ts: Long,
      ctx: WindowAssigner.WindowAssignerContext): java.util.List[TimeWindow] = {

    // rounding down by 30 seconds
    val startTime = ts - (ts % windowSize)
    val endTime = startTime + windowSize
    print("assignWindows ts: ", timeformater.format(ts), "windows: ", timeformater.format(startTime), timeformater.format(endTime))
    // emitting the corresponding time window
    //println(o, startTime, endTime)
    Collections.singletonList(new TimeWindow(startTime, endTime))
  }

  override def getDefaultTrigger(
      env: environment.StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    EventTimeTrigger.create()
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }

  override def isEventTime = true
}

/** A trigger that fires early. The trigger fires at most every second. */
class OneSecondIntervalTrigger
    extends Trigger[SensorReading, TimeWindow] {

  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  override def onElement(r: SensorReading, timestamp: Long, window: TimeWindow,
      ctx: Trigger.TriggerContext): TriggerResult = {

    // firstSeen will be false if not set yet
    val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(
      new ValueStateDescriptor[Boolean]("firstSeen", createTypeInformation[Boolean]))

    // register initial timer only for first element
    if (!firstSeen.value()) {
      // compute time for next early firing by rounding watermark to second
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      ctx.registerEventTimeTimer(t)
      // register timer for the window end
      ctx.registerEventTimeTimer(window.getEnd)
      firstSeen.update(true)
      println("onElement firstSeen ", t)
    }
    // Continue. Do not evaluate per element
    TriggerResult.CONTINUE
  }

  override def onEventTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("onEventTime", timeformater.format(timestamp), timestamp)
    if (timestamp == window.getEnd) {
      println("onEventTime window.getEnd", timeformater.format(window.getEnd), window.getEnd)
      // final evaluation and purge window state
      TriggerResult.FIRE_AND_PURGE
    } else {
      // register next early firing timerregisterEventTimeTimer
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))

      println("onEventTime t", timeformater.format(t), t)
      if (t < window.getEnd) {
        ctx.registerEventTimeTimer(t)
      }
      // fire trigger to evaluate window
      TriggerResult.FIRE
    }
  }

  override def onProcessingTime(timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("onProcessingTime")
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // clear trigger state
    println("clear")
    val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", createTypeInformation[Boolean]))
    firstSeen.clear()
  }
}

/** A window function that counts the readings per sensor and window.
  * The function emits the sensor id, window end, time of function evaluation, and count. */
class CountFunction
    extends ProcessWindowFunction[SensorReading, (String, String, Long, Long, Int), String, TimeWindow] {

  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
  override def process(
      key: String,
      ctx: Context,
      readings: Iterable[SensorReading],
      out: Collector[(String, String, Long, Long, Int)]): Unit = {

    // count readings
    val cnt = readings.count(_ => true)
    // get current watermark
    val evalTime = ctx.currentWatermark
    // emit result
    println("ProcessWindowFunction",  timeformater.format(ctx.window.getEnd),  timeformater.format(evalTime))
    out.collect(("xxxxx",key, ctx.window.getEnd, evalTime, cnt))
  }
}
