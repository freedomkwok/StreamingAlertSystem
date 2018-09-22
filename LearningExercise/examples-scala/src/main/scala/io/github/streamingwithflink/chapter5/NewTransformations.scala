package io.github.streamingwithflink.chapter5

import io.github.streamingwithflink.util._
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/** Object that defines the DataStream program in the main() method */
object NewTransformations {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(5000L)

    // ingest sensor stream
    val readings: DataStream[TestSourceReading] = env
      // SensorSource generates random temperature readings
      .addSource(new TestSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new TestSourceTimeAssigner)

    // filter out sensor measurements from sensors with temperature under 25 degrees
    val filteredSensors: DataStream[TestSourceReading] = readings
      .filter( r =>  r.temperature < 5)

    val filteredSensors5: DataStream[TestSourceReading] = readings
      .filter( r =>  r.temperature >= 5)

    // the above filter transformation using a UDF
    // val filteredSensors: DataStream[SensorReading] = sensorData
    //   .filter(new TemperatureFilter(25))

    // project the id of each sensor reading
    val sensorIds: DataStream[(String, String, Double ,Double)] = filteredSensors
      .map(r => ("print", r.id, r.sec, r.temperature ))

    val sensorIds5: DataStream[(String, String, Double ,Double, String)] = filteredSensors5
      .map(r => ("print", r.id, r.sec, r.temperature, "special"))

    // the above map transformation using a UDF
    // val sensorIds2: DataStream[String] = sensorData
    //   .map(new ProjectionMap)

    // the above flatMap transformation using a UDF
    // val splitIds: DataStream[String] = sensorIds
    //  .flatMap( new SplitIdFlatMap )

    // print result stream to standard out
    //sensorIds.print()
    //sensorIds5.print()

    val result : DataStream[Double] = sensorIds.timeWindowAll(Time.seconds(2)).sum(3).map(r => r._3)
    result.print()


    // execute application
    env.execute("Basic Transformations Example")
  }

  /** User-defined FilterFunction to filter out SensorReading with temperature below the threshold */
  class TemperatureFilter(threshold: Long) extends FilterFunction[SensorReading] {

    override def filter(r: SensorReading): Boolean = r.temperature >= threshold

  }

  /** User-defined MapFunction to project a sensor's id */
  class ProjectionMap extends MapFunction[SensorReading, String] {

    override def map(r: SensorReading): String  = r.id

  }

  /** User-defined FlatMapFunction that splits a sensor's id String into a prefix and a number */
  class SplitIdFlatMap extends FlatMapFunction[String, String] {

    override def flatMap(id: String, collector: Collector[String]): Unit = id.split("_")

  }

}
