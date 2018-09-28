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
    val rand = new Random()

    val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
    // initialize 10 clients
    val clientIds = (1 to 2).map {
      i => 1 + rand.nextInt(2)
    }
    // emit data until being canceled
    if(running) {
      // initialize random number generator

      // update temperature
      // get current time
      val curTimeInstance = Calendar.getInstance
      val begin = curTimeInstance.getTime
      curTimeInstance.add(Calendar.SECOND, (rand.nextInt(20) + 180)/2)
      val end = curTimeInstance.getTime
      val alertUse1 =  AlertUse(
        1, //clientID
        1, //AlertUse
        1, //EnttyTypeID
        1, //EntityID
        1,  //AlertTypeID
        rand.nextInt(6) * 5000L, //interval t x 30sec
        timeformater.format(begin), //begin time
        timeformater.format(end)
      )
      srcCtx.collect(alertUse1)

      curTimeInstance.add(Calendar.SECOND, (rand.nextInt(20) + 180)/2)
      val alertUse2 = AlertUse(
        1,  //clientID
        2, //AlertUse
        2, //EnttyTypeID
        2, //EntityID
        2,  //AlertTypeID
        rand.nextInt(6) * 5000L, //interval t x 30sec
        timeformater.format(begin), //begin time
        timeformater.format(end)
      )
      srcCtx.collect(alertUse2)

      val begin3 = curTimeInstance.getTime
      curTimeInstance.add(Calendar.SECOND, (rand.nextInt(20) + 180)/2)
      val alertUse3 = AlertUse(
        3,  //clientID
        3, //AlertUse
        3, //EnttyTypeID
        3, //EntityID
        3,  //AlertTypeID
        rand.nextInt(6) * 5000L, //interval t x 30sec
        timeformater.format(begin), //begin time
        timeformater.format(curTimeInstance.getTime)
      )
      srcCtx.collect(alertUse3)
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

//      val begin2 = curTimeInstance.getTime
//      curTimeInstance.add(Calendar.MINUTE, (rand.nextInt(5) + 1)/2)
//      srcCtx.collect(
//        AlertUse(
//          1, //clientID
//          1, //AlertUse
//          2, //EnttyTypeID
//          1, //EntityID
//          1,  //AlertTypeID
//          rand.nextInt(6) * 5000L, //interval t x 30sec
//          timeformater.format(begin2), //begin time
//          timeformater.format(curTimeInstance.getTime)
//        ))
      // wait for 100 ms
      Thread.sleep(50000)
      println("output new source ", running )
//      val latebegin = curTimeInstance.getTime
//      curTimeInstance.add(Calendar.SECOND, (rand.nextInt(20) + 30)/2)
//
//      val alertUselate =  AlertUse(
//        1, //clientID
//        1, //AlertUse
//        1, //EnttyTypeID
//        1, //EntityID
//        1,  //AlertTypeID
//        rand.nextInt(6) * 5000L, //interval t x 30sec
//        timeformater.format(latebegin), //begin time
//        timeformater.format(curTimeInstance.getTime)
//      )
//      srcCtx.collect(alertUselate)
    }
  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
