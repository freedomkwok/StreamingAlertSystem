package com.getcake.automation.data.sources

import com.getcake.sourcetype.StreamData
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class TestKinesisAssigner extends BoundedOutOfOrdernessTimestampExtractor[StreamData](Time.seconds(2)) {

  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: StreamData): Long = r.request_date


}
