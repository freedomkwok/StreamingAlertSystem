package com.getcake

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.util._
import org.apache.flink.streaming.connectors.kinesis._
import org.apache.flink.streaming.connectors.kinesis.config._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import java.util._

import com.getcake.automation.data.sources._
import com.getcake.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object StreamingAlertSystem {

  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setParallelism(1)
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

//    val consumerConfig : Properties  = new Properties()
//    consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-west-2")
//    consumerConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "")
//    consumerConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "")
//    consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")
//    consumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, ThirtySecondsWindows"500")

    val testKinesisStream : org.apache.flink.streaming.api.scala.DataStream[StreamData] = env.addSource(new KinesisSourceGenerator)
                               .assignTimestampsAndWatermarks(new TestKinesisAssigner)
    val countsPerThirtySecs = testKinesisStream
      .keyBy(_.client_id)
      .window(new CustomWindows(1000))
      .trigger(new OneSecondIntervalTrigger)
      .process(new MyCountFunction)

      countsPerThirtySecs.print()
      env.execute("flink aggregate")
//    val alertUseStream = env.addSource(new AlertUseDataSource)
//                            .assignTimestampsAndWatermarks(new AlertUseAssigner)
//     alertUseStream.print()
//    testKinesisStream.connect(alertUseStream)
  }

  /** A custom window that groups events into 30 second tumbling windows. */
  class CustomWindows(windowPeriod: Long) extends WindowAssigner[Object, TimeWindow]
  {
    val windowSize: Long = windowPeriod

    override def assignWindows(o: Object, ts: Long, ctx: WindowAssigner.WindowAssignerContext): java.util.List[TimeWindow] = {
      val startTime = ts - (ts % windowSize)
      val endTime = startTime + windowSize
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

  class OneSecondIntervalTrigger extends Trigger[StreamData, TimeWindow]
  {
    override def onElement(r: StreamData, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // firstSeen will be false if not set yet
      val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", createTypeInformation[Boolean]))

      // register initial timer only for first element
      if (!firstSeen.value()) {
        // compute time for next early firing by rounding watermark to second
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        ctx.registerEventTimeTimer(t)
        // register timer for the window end
        ctx.registerEventTimeTimer(window.getEnd)
        firstSeen.update(true)
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
      val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", createTypeInformation[Boolean]))
      firstSeen.clear()
    }
  }

  class MyCountFunction
    extends ProcessWindowFunction[StreamData, Int, String, TimeWindow] {
    override def process(key: String, ctx: Context, readings: Iterable[StreamData], out: Collector[Int]): Unit = {
      // count readings
      val cnt = readings.count(_ => true)
      // get current watermark
      val evalTime = ctx.currentWatermark

      // emit result
      out.collect(10)
    }
  }

}
