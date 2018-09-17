package com.getcake

import org.apache.flink.streaming.api.scala.{DataStream, _}
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
import com.getcake.aggregation.trigger._
import com.getcake.aggregation.window._
import com.getcake.aggregation.function._
import com.getcake.mappers.TrafficAlertFilterFunction
import com.getcake.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple.Tuple3
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

    val testKinesisStream: DataStream[StreamData] = env.addSource(new KinesisSourceGenerator)
      .assignTimestampsAndWatermarks(new TestKinesisAssigner)


    val alertUseStream = env.addSource(new AlertUseDataSource)
                            .assignTimestampsAndWatermarks(new AlertUseAssigner)
    alertUseStream.print()

    val activeAlertStreamData = testKinesisStream.connect(alertUseStream)
      .keyBy(_.client_id, _.ClientID)
      .process(new TrafficAlertFilterFunction)
    //testKinesisStream.print()
    //      .keyBy(_.client_id)
    //      .window(new CustomWindows(1000))
    //      .trigger(new OneSecondIntervalTrigger)
    //      .process(new CustomProcessFunction)
    activeAlertStreamData.print()


    env.execute("flink aggregate")
  }

  }

