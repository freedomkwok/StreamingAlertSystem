
import Model.{ServerHeartbeat, SessionOnInstanceLevel, SessionOnProductLevel}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream, KeyedStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala.{DataSet, createTypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Properties
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

object BasicTimeOutStreamTest {
  def main(args: Array[String]): Unit = {


    implicit val valueType: TypeInformation[ServerHeartbeat] = createTypeInformation[ServerHeartbeat]
//    println(valueType.isKeyType)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableForceAvro()

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.registerType(classOf[ServerHeartbeat])
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset", "latest")

//
    val inputStream = env.socketTextStream("localhost", 8888)

    val dataStream: DataStream[ServerHeartbeat] = inputStream
      .filter(x => x.nonEmpty)
      .map(data => {
      //println(data)
      val str = data.split(",")
      ServerHeartbeat(str(0), str(1), str(2), str(3), str(4).toInt)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ServerHeartbeat](Time.milliseconds(10000L)) {
      override def extractTimestamp(element: ServerHeartbeat): Long = element.timestamp * 1000L
    })


    //dataStream.print()
    val customerStreamData = dataStream.keyBy(new KeySelector[ServerHeartbeat, String]() {
      @Override
      def getKey(s: ServerHeartbeat): String  = {
        s.instanceId + s.productId + s.customerId
      }}
    )

    class InstanceProcessor() extends KeyedProcessFunction[String, ServerHeartbeat, SessionOnInstanceLevel] {
        lazy val sessionState: ListState[Long] = {
        getRuntimeContext.getListState(new ListStateDescriptor[Long]("instanceLevelList", classOf[Long]))
      }

      override def processElement(value: ServerHeartbeat, ctx: KeyedProcessFunction[String, ServerHeartbeat, SessionOnInstanceLevel]#Context, out: Collector[SessionOnInstanceLevel]): Unit = {
        var list = sessionState.get().asScala.toList
        if(list.isEmpty || value.timestamp > list.last) {
          sessionState.add(value.timestamp)
          list = sessionState.get().asScala.toList
        }

        if (list.length > 1) {
          out.collect(SessionOnInstanceLevel(value.instanceId, value.productId, list(1) - list(0), value.timestamp))
          sessionState.clear()
          sessionState.addAll(list.drop(1).asJava)
        }
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, ServerHeartbeat, SessionOnInstanceLevel]#OnTimerContext, out: Collector[SessionOnInstanceLevel]): Unit = {
        super.onTimer(timestamp, ctx, out)
      }
    }

    val result1 = customerStreamData.process(new InstanceProcessor())
//    result1.print()

    result1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SessionOnInstanceLevel](Time.milliseconds(5000L)) {
      override def extractTimestamp(element: SessionOnInstanceLevel): Long = element.timestamp * 1000L
    }).keyBy(new KeySelector[SessionOnInstanceLevel, String]() {
        @Override
        def getKey(s: SessionOnInstanceLevel): String  = {
          s.productId
        }}
      ).process(new KeyedProcessFunction[String, SessionOnInstanceLevel, SessionOnProductLevel] {
        lazy val mapState: MapState[String, Long] = {
          getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("productSession", classOf[String], classOf[Long]))
        }

        lazy val nextEnd: ValueState[Long] = getRuntimeContext.getState[Long]( new ValueStateDescriptor[Long]("lastAppearSession", classOf[Long]))

        override def processElement(value: SessionOnInstanceLevel, ctx: KeyedProcessFunction[String, SessionOnInstanceLevel, SessionOnProductLevel]#Context, out: Collector[SessionOnProductLevel]): Unit = {
          val nextEndInMs = value.timestamp * 1000L

          val _zoneId = ZoneId.of("America/Los_Angeles")
          val curTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(value.timestamp * 1000L), _zoneId)
          val processTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx.timerService().currentProcessingTime()), _zoneId)
          val wm = ctx.timerService().currentWatermark()
          val waterMark = if( wm > 0 ) ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx.timerService().currentProcessingTime()), _zoneId) else wm

//          println(curTime)
//          println(processTime)

          mapState.put(value.id, (if(mapState.contains(value.id)) mapState.get(value.id) else 0L) + value.totalTime)
          if(nextEnd.value() == null) { // not late event
            {
              val nextTrigger = nextEndInMs + 10 * 1000L
              println(ZonedDateTime.ofInstant(Instant.ofEpochMilli(nextTrigger), _zoneId))
              ctx.timerService().registerEventTimeTimer(nextTrigger)
              nextEnd.update(value.timestamp)
            }
          }

        }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SessionOnInstanceLevel, SessionOnProductLevel]#OnTimerContext, out: Collector[SessionOnProductLevel]): Unit = {
        println("OnTimer")
        var totalSession = 0L
        mapState.keys.forEach(k => {
          totalSession += mapState.get(k)
        })
        out.collect(SessionOnProductLevel(ctx.getCurrentKey, totalSession, ctx.timestamp.longValue(), ctx.timerService().currentWatermark()))
      }
      }).print()


    env.execute()
  }
}

/*
Instance11111,s3,s3,ngenda1xxx,1709703073
Instance11111,s3,s3,ngenda1xxx,1709703074
Instance11111,s3,s3,ngenda1xxx,1709703075
Instance11111,s3,s3,ngenda1xxx,1709703084

Instance11112,s3,s3,ngenda1xxx,1709703073
Instance11112,s3,s3,ngenda1xxx,1709703076
Instance11112,s3,s3,ngenda1xxx,1709703079

Instance11113,s3,s3,ngenda1xxx,1709703073
Instance11113,s3,s3,ngenda1xxx,1709703074
Instance11113,s3,s3,ngenda1xxx,1709703075

Instance11114,cloud,cloud,ngenda2xx,1709703073
Instance11114,cloud,cloud,ngenda2xx,1709703074
Instance11114,cloud,cloud,ngenda2xx,1709703075

Instance11115,cloud,cloud,ngenda2xx,1709703073
Instance11115,cloud,cloud,ngenda2xx,1709703074
Instance11115,cloud,cloud,ngenda2xx,1709703075

Instance11115,cloud,cloud,ngenda2xx,1709703088
Instance11111,s3,s3,ngenda1xxx,1709703093
 */