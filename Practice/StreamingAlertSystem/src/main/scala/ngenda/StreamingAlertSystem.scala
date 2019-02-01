package com.ngenda

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, _}
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.util._
import org.apache.flink.streaming.connectors.kinesis._
import org.apache.flink.streaming.connectors.kinesis.config._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import com.ngenda.automation.data.sources._
import com.ngenda.aggregation.windows._
import com.ngenda.aggregation.triggers._
import com.ngenda.aggregation.windowassigners._
import com.ngenda.aggregation.functions._
import com.ngenda.mappers.{TrafficAlertFilterFunction, TrafficCapMapper}
import com.ngenda.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.createTypeInformation

import scala.util.Random
import org.apache.flink.contrib.streaming.state._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

  object StreamingAlertSystem {
                                      //clientid, entitiy_id, entitiy_type_id. count
    val entityCapStatuses: OutputTag[(Int, Int, Int, Int)] =
      new OutputTag[(Int, Int, Int, Int)]("entity_cap_status")

    val alertUseStatus: OutputTag[(Int, Int, Int, Long, Long)] =
      new OutputTag[(Int, Int, Int, Long, Long)]("alert_use_status")

  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(15 * 1000)
    env.setParallelism(3)
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.setStateBackend(new FsStateBackend("hdfs://localhost:19000/flink_checkpoint1", true))
    env.getConfig.setAutoWatermarkInterval(1000L)


    //    val consumerConfig : Properties  = new Properties()
    //    consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-west-2")
    //    consumerConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "")
    //    consumerConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "")
    //    consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")
    //    consumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, ThirtySecondsWindows"500")

    val testKinesisStream: DataStream[StreamData] = env.addSource(new KinesisSourceGenerator)
      .assignTimestampsAndWatermarks(new TestKinesisAssigner)
      .keyBy(_.client_id)

    val alertUseStream = env.addSource(new AlertUseDataSource)
                            .assignTimestampsAndWatermarks(new AlertUseAssigner)
                            .keyBy(_.ClientID)

    //alertUseStream.process().getSideOutput()

    val activeAlertStreamData = testKinesisStream.connect(alertUseStream)
      .keyBy(_.client_id, _.ClientID)
      .process(new TrafficCapMapper(true))

    val localOutput = activeAlertStreamData
                      .keyBy(_._2)
                      .window(new MiniBatchIntervalWindowAssigner(0))  //CustomWindowAssigner
                      .trigger(new CapTrigger)
                      .process(new CapProcessFunction)

    println("-----------Result----------------")
    localOutput.print()
    localOutput.getSideOutput(entityCapStatuses)

    env.execute("flink aggregate")
  }

  }
