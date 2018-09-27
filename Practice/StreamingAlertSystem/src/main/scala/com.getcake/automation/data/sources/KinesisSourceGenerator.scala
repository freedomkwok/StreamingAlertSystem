package com.getcake.automation.data.sources

import java.text.SimpleDateFormat
import java.util.Calendar

import com.getcake.sourcetype.StreamData
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class KinesisSourceGenerator extends RichParallelSourceFunction[StreamData] {
  var running: Boolean = true
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[StreamData]): Unit = {

    // initialize random number generator
    val rand = new Random()

    // initialize 10 clients
    val clientIds = (1 to 3).map {
      i => i
    }

    val campaignIds = (1 to 4).map {
      i => rand.nextInt(3) + 1
    }

    val publisherIds = (1 to 4).map {
      i => rand.nextInt(3) + 1
    }

    val offerIds = (1 to 4).map {
      i => rand.nextInt(3) + 1
    }
    // emit data until being canceled
    while (running) {

      // update temperature
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis
      //println(timeformater.format(curTime))
      // emit new SensorReading
      clientIds.foreach(clientId => {
        val data = StreamData(
          Option(null), clientId, curTime, curTime,
          Option(publisherIds(rand.nextInt(3))),
          Option(offerIds(rand.nextInt(3))),
          Option(campaignIds(rand.nextInt(3)))
        )
        srcCtx.collect(data)})

      // wait for 100 ms
      Thread.sleep(500)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
