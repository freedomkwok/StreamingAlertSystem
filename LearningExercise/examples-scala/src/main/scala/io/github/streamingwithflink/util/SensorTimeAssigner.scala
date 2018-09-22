package io.github.streamingwithflink.util

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Assigns timestamps to SensorReadings based on their internal timestamp and
  * emits watermarks with five seconds slack.
    */
  class SensorTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {

  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
    /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: SensorReading): Long = {
    println("extractTimestamp", r.id, timeformater.format(r.timestamp), r.timestamp)
    r.timestamp
  }
}
