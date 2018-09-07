package com.getcake

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

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

import org.apache.flink.streaming.api.datastream.DataStream

/**
  * An example that shows how to read from and write to Kafka. This will read String messages
  * from the input topic, prefix them by a configured prefix and output to the output topic.
  *
  * Please pass the following arguments to run the example:
  * {{{
  * --input-topic test-input
  * --output-topic test-output
  * --bootstrap.servers localhost:9092
  * --zookeeper.connect localhost:2181
  * --group.id myconsumer
  * }}}
  */

object StreamingAlertSystem {

  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    // create a checkpoint every 5 seconds
    //env.getCheckpointConfig
    // make parameters available in the web interface
    //env.getConfig.setGlobalJobParameters(params)

    val consumerConfig : Properties  = new Properties()
    consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-west-2")
    consumerConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "")
    consumerConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "")
    consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")
    consumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "500")

    var kinesisStream = env.addSource(new FlinkKinesisConsumer("ConversionScore", new SchemaEventSchema, consumerConfig))
      kinesisStream
        .map{ (x) => {
          println(x.clientID)
          (x, 1)
        }}
        .keyBy(_._1.clientID)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
          .sum(1)

    env.execute("flink aggregate")
  }
}
