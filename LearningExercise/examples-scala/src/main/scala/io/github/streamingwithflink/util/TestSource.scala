package io.github.streamingwithflink.util

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
  * Flink SourceFunction to generate SensorReadings with random temperature values.
  *
  * Each parallel instance of the source simulates 10 sensors which emit one sensor
  * reading every 100 ms.
  *
  * Note: This is a simple data-generating source function that does not checkpoint its state.
  * In case of a failure, the source does not replay any data.
  */
class TestSource extends RichParallelSourceFunction[TestSourceReading] {

  // flag indicating whether source is still running.
  var running: Boolean = true
  var count : Int = 0

  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[TestSourceReading]): Unit = {

    // initialize random number generator
    val rand = new Random()
    // look up index of this parallel task
      val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
        val next = rand.nextInt(5)
        // initialize sensor ids and temperatures
        var curFTemp = (1 to 5).map {
          i =>
            ("sensor_" + (taskIdx * 10 + i), next/1 , next / 10)
        }

        // emit data until being canceled
        while (running) {

      // update temperature
      curFTemp = curFTemp.map( t => (t._1, t._2, t._3))
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      curFTemp.foreach( t => srcCtx.collect(TestSourceReading(t._1, curTime, t._2, t._3)))

      // wait for 100 ms
      Thread.sleep(2000)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }

}