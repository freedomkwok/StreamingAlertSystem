package com.getcake.automation.data.sources

import com.getcake.sourcetype.AlertUse
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class AlertUseAssigner extends BoundedOutOfOrdernessTimestampExtractor[AlertUse](Time.seconds(2)) {
  lazy val formater = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: AlertUse): Long = {
    val eventTime = formater.parse(r.AlertUseBegin)
    //println("eventTime ", eventTime, eventTime.getTime)
    eventTime.getTime
  }
}

