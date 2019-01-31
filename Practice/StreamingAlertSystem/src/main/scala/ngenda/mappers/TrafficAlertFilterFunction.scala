package com.ngenda.mappers

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ngenda.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.joda.time.Interval

class TrafficAlertFilterFunction(useCheckPoint: Boolean) extends CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long, Int, Long)] {//} with CheckpointedFunction {
  //Map ClientID entity_id AlertUseId
  lazy val keyAlertUseDescriptor = new MapStateDescriptor[(Int, Int, Int), (Boolean, Long, Long, Int)]("alertUseMapper", createTypeInformation[(Int, Int, Int)], createTypeInformation[(Boolean, Long, Long, Int)])
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  lazy val alertUseMapper: MapState[(Int, Int, Int), (Boolean, Long, Long, Int)] = getRuntimeContext.getMapState(keyAlertUseDescriptor)

  lazy val loadedCheckPoint: ValueState[Boolean] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("loadedCheckPoint", Types.of[Boolean])
    )

  lazy val loadingCheckPoint: ValueState[Boolean] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("loadingCheckingPoint", Types.of[Boolean])
    )

  var alertUseMapperCheckPoint: MapState[(Int, Int, Int), (Boolean, Long, Long, Int)] = _

  override def processElement1(
    streamData: StreamData,
    ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long, Int, Long)]#Context,
    out: Collector[(String, Int, Int, Int, Long, Long, Int, Long)]): Unit = {

    //waitForloadingCheckPoint()

    val publisherKey : (Int, Int, Int) = (streamData.client_id, streamData.publisher_id.getOrElse(0), 1)
    val offerKey : (Int, Int, Int) = (streamData.client_id, streamData.offer_id.getOrElse(0), 2)
    val campaignKey : (Int, Int, Int) = (streamData.client_id, streamData.campaign_id.getOrElse(0), 3)

    //println(ctx.timestamp(), ctx.timerService().currentWatermark(), ctx.timerService().currentProcessingTime())
    // check if we may forward the reading
    val now = ctx.timestamp()

    //println(timeformater.format(now), publisherKey, offerKey, campaignKey)
    val watermark = ctx.timerService().currentWatermark()
    val processTime = ctx.timerService().currentProcessingTime()
//    println("streamData timestamp:", now, "actual time:", streamData.request_date, timeformater.format(streamData.request_date))
//    println("watermark", watermark , timeformater.format(watermark))
//    println("processtime", processTime, timeformater.format(processTime))
//    println()

   // print(" publisherKey")

    if(alertUseMapper.contains(publisherKey)) {
      val hasPublisher = alertUseMapper.get(publisherKey)
    //  print(" found")
      if(hasPublisher._2 <= now && now <= hasPublisher._3)
        out.collect(("filteredData",
                      streamData.client_id,
                      streamData.publisher_id.getOrElse(0),
                      1,
                      hasPublisher._2,
                      hasPublisher._3,
                      hasPublisher._4,
          streamData.request_date
        ))
    }
    else
    {
   //   print(" -----")
    }

    //print(", offerKey")
    if(alertUseMapper.contains(offerKey)) {
      val hasOffer = alertUseMapper.get(offerKey)
     // print(" found")
      if(hasOffer._2 <= now && now <= hasOffer._3)
        out.collect(("filteredData",
                      streamData.client_id,
                      streamData.offer_id.getOrElse(0),
                      2,
                      hasOffer._2,
                      hasOffer._3,
          hasOffer._4,
          streamData.request_date))
    }
//    else
//    {
//      print(" -----")
//    }
//
   // print(", campaignKey")
    if(alertUseMapper.contains(campaignKey)) {
   //   print(" found")
      val hasCampaign = alertUseMapper.get(campaignKey)
      if(hasCampaign._2 <= now && now <= hasCampaign._3)
        out.collect(("filteredData",
                    streamData.client_id,
                    streamData.campaign_id.getOrElse(0),
                    3,
                    hasCampaign._2,
                    hasCampaign._3,
          hasCampaign._4,
          streamData.request_date))
    }
    else
    {
    //  print(" -----")
    }
    //println()

  }

  override def processElement2(
      alertUse: AlertUse,
      ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long, Int, Long)]#Context,
      out: Collector[(String, Int, Int, Int, Long, Long, Int, Long)]): Unit = {

    //waitForloadingCheckPoint()
    // set disable forward timer
    val alertUseKey : (Int, Int, Int) = (alertUse.ClientID, alertUse.EntityID, alertUse.EntityTypeID)
    val begin = timeformater.parse(alertUse.AlertUseBegin).getTime
    val end = timeformater.parse(alertUse.AlertUseEnd).getTime
    println("==========================================================================================================================")
    println("processElement2 ", " client_id: ",alertUseKey._1, "entity_id: ", alertUseKey._2 , "entity_type_id: ", alertUseKey._3)
    println("begin: ", timeformater.format(begin)," (" + begin.toString + ")", " end: ", timeformater.format(end) + " (" + end.toString + ")")
    val t = ctx.timestamp()

    println("curWaterMark", t, timeformater.format(t))
    println()

    try {
      if(!alertUseMapper.contains(alertUseKey)) {
        // println("Registered AlertUse: ", begin, end)
        println("Fresh alertUseMapper: ", alertUseKey)
        alertUseMapper.put(alertUseKey, (false, begin, end, 0))
        //
        //      var current = ctx.timerService().currentWatermark()
        //        current = current + (1000 - (current % 1000))
        ctx.timerService().registerEventTimeTimer(end) // invoke endtime

        //println("timenow: ", timeformater.format(current), current)
      }
      else
        println("new_Entry alertUse: ", alertUse, timeformater.format(ctx.timestamp()))
    }
    catch {
      case e: Exception => {
        println("Exception finding key ", e, alertUseKey)
      }
    }

  }

  override def onTimer(
      ts: Long,
      ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long, Int, Long)]#OnTimerContext,
      out: Collector[(String, Int, Int, Int, Long, Long, Int, Long)]): Unit = {

      //remove list from alertUseMapper
      val mapIterator = alertUseMapper.iterator()

      println("onTimer registered ts:", ts, timeformater.format(ts))
      println("onTimer ", "watermark: ", timeformater.format(ctx.timerService().currentWatermark()), "processTime: ", timeformater.format(ctx.timerService().currentProcessingTime()))

      while(mapIterator.hasNext) {
          val item = mapIterator.next()
          if(item.getValue._3 >= ts) //if endtime has arrive{
          {
            val originValue = item.getValue
            println("onTimer registerTime: ", originValue._3, timeformater.format(originValue._3))
            val key = item.getKey
            alertUseMapper.put(key, (false, originValue._2, originValue._3, 0))
            println()
          }
      }
    val timeInstance = Calendar.getInstance
      val end = timeformater.format(timeInstance.getTime)
      println("Refresh MapperTime:", end)
  }

  def waitForloadingCheckPoint() = {
      if(!loadedCheckPoint.value()) {
        println("waitForloadingCheckPoint")
        if (loadingCheckPoint.value())
          while(!loadedCheckPoint.value()) {
            println("waiting")
            Thread.sleep(500)
          }
        else {
          loadingCheckPoint.update(true)
          var counter = 0
          val mapIterator = alertUseMapperCheckPoint.iterator()
          while (mapIterator.hasNext) {
            val item = mapIterator.next()
            this.alertUseMapper.put(item.getKey, item.getValue)
            counter += 1
          }

          loadedCheckPoint.update(true)
          println("checkpoint loaded: ", counter)
        }
      }
  }

//  override def initializeState(initCtx: FunctionInitializationContext): Unit = {
//    this.alertUseMapperCheckPoint = initCtx.getKeyedStateStore.getMapState(keyAlertUseDescriptor)
//  }
//
//  override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
//    val mapIterator = alertUseMapper.iterator()
//    while(mapIterator.hasNext) {
//      val item = mapIterator.next()
//      this.alertUseMapperCheckPoint.put(item.getKey, item.getValue)
//    }
//    println("saving checkingpoint", ctx.getCheckpointId, ctx.getCheckpointTimestamp)
//  }
}