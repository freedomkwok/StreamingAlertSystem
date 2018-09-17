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
    val clientIds = (1 to 2).map {
      i => 1 + rand.nextInt(2)
    }

    // emit data until being canceled
    while(running) {
      // update temperature
      // get current time
      val curTimeInstance = Calendar.getInstance

      // emit new SensorReading
//      clientIds.foreach(clientId => {
      ////        val begin = curTimeInstance.getTime
      ////        curTimeInstance.add(Calendar.HOUR, rand.nextInt(30))
      ////
      ////        srcCtx.collect(
      ////          AlertUse(
      ////            clientId,
      ////            rand.nextInt(5), //AlertUse
      ////            rand.nextInt(3), //EnttyTypeID
      ////            rand.nextInt(3), //EntityID
      ////            rand.nextInt(5),  //AlertTypeID
      ////            rand.nextInt(20) * 30000L, //interval t x 30sec
      ////            timeformater.format(begin), //begin time
      ////            timeformater.format(curTimeInstance.getTime)
      ////          ))
      ////      })
      val begin = curTimeInstance.getTime
      curTimeInstance.add(Calendar.HOUR, rand.nextInt(30))
      srcCtx.collect(
                  AlertUse(
                    1,
                    1, //AlertUse
                    1, //EnttyTypeID
                    1, //EntityID
                    1,  //AlertTypeID
                    rand.nextInt(20) * 30000L, //interval t x 30sec
                    timeformater.format(begin), //begin time
                    timeformater.format(curTimeInstance.getTime)
                  ))

      val begin1 = curTimeInstance.getTime
      curTimeInstance.add(Calendar.HOUR, rand.nextInt(30))
      srcCtx.collect(
        AlertUse(
          2,
          2, //AlertUse
          2, //EnttyTypeID
          2, //EntityID
          2,  //AlertTypeID
          rand.nextInt(20) * 30000L, //interval t x 30sec
          timeformater.format(begin1), //begin time
          timeformater.format(curTimeInstance.getTime)
        ))

      // wait for 100 ms
      Thread.sleep(600000)
    }
  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
