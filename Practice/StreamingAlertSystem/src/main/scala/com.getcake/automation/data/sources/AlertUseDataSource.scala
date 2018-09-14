package com.getcake.automation.data.sources

import java.util.Calendar
import java.text.SimpleDateFormat

import com.getcake.sourcetype._
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class AlertUseDataSource extends RichParallelSourceFunction[AlertUse] {
  var running: Boolean = true
  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[AlertUse]): Unit = {

    // initialize random number generator
    val rand = new Random()

    val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    // initialize 10 clients
    val AlertUseIds = (1 to 2).map {
      i => rand.nextInt(20)
    }

    // emit data until being canceled
    while(running) {
      // update temperature
      // get current time
      val curTimeInstance = Calendar.getInstance

      // emit new SensorReading
      AlertUseIds.foreach(clientId => {
        val begin = curTimeInstance.getTime
        curTimeInstance.add(Calendar.HOUR, rand.nextInt(30))
        srcCtx.collect(
          AlertUse(
            clientId,
            rand.nextInt(5), //type
            rand.nextInt(20) * 30000L, //interval t x 30sec
            timeformater.format(begin), //begin time
            timeformater.format(curTimeInstance.getTime)
          ))
      })
      // wait for 100 ms
      Thread.sleep(60000)
    }
  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
