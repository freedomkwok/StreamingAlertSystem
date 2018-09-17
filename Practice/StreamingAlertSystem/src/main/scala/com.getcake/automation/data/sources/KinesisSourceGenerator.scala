package com.getcake.automation.data.sources

import java.util.Calendar

import com.getcake.sourcetype.StreamData
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class KinesisSourceGenerator extends RichParallelSourceFunction[StreamData] {
  var running: Boolean = true

  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[StreamData]): Unit = {

    // initialize random number generator
    val rand = new Random()

    // initialize 10 clients
    val clientIds = (1 to 10).map {
      i => rand.nextInt(2)
    }

    val campaignIds = (1 to 10).map {
      i => rand.nextInt(3)
    }

    val publisherIds = (1 to 10).map {
      i => rand.nextInt(3)
    }

    val offerIds = (1 to 10).map {
      i => rand.nextInt(3)
    }
    // emit data until being canceled
    while (running) {

      // update temperature
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      clientIds.foreach(clientId => srcCtx.collect(
                                          StreamData(
                                              Option(null), clientId, curTime, curTime,
                                              Option(publisherIds(rand.nextInt(10))),
                                              Option(offerIds(rand.nextInt(10))),
                                              Option(campaignIds(rand.nextInt(10)))
                                          )))

      // wait for 100 ms
      Thread.sleep(1000)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
