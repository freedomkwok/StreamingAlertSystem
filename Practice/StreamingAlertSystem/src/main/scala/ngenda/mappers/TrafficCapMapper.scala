package com.ngenda.mappers

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ngenda.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.state.{ListStateDescriptor, ListState, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.joda.time.Interval

class TrafficCapMapper(useCheckPoint: Boolean) extends CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long, Int, Long)] with CheckpointedFunction {
//Map ClientID entity_id AlertUseId
  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")
  private val keyAlertUseDescriptor = new MapStateDescriptor[(Int, Int, Int), (Boolean, Long, Long, Int)]("alertUseMapper", createTypeInformation[(Int, Int, Int)], createTypeInformation[(Boolean, Long, Long, Int)])

  var alertUseMapper: MapState[(Int, Int, Int), (Boolean, Long, Long, Int)] = null

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

    val watermark = ctx.timerService().currentWatermark()
    val processTime = ctx.timerService().currentProcessingTime()

//    println("streamData timestamp:", now, "actual time:", streamData.request_date, timeformater.format(streamData.request_date))
//    println("watermark", watermark , timeformater.format(watermark))
//    println("processtime", processTime, timeformater.format(processTime))
      ///println("streamData: ", streamData)
    // print(" publisherKey")

    if(alertUseMapper.contains(publisherKey)) {
      val hasPublisher = alertUseMapper.get(publisherKey)

      if(hasPublisher._2 <= now && now <= hasPublisher._3)
        out.collect(("filteredData",
          streamData.client_id,
          streamData.publisher_id.getOrElse(0),
          1,
          hasPublisher._2,
          hasPublisher._3,
          hasPublisher._4,
          streamData.request_date))
    }
    else
    {

    }

    //print(", offerKey")
    if(alertUseMapper.contains(offerKey)) {
      val hasOffer = alertUseMapper.get(offerKey)

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
        else
        {

        }
    //
    // print(", campaignKey")
    if(alertUseMapper.contains(campaignKey)) {
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

    }


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
    println("processElement2 ", " client_id: ",alertUseKey._1, "entity_id: ", alertUseKey._2 , "entity_type_id: ", alertUseKey._3, "cap", alertUse.Cap )
    println("begin: ", timeformater.format(begin)," (" + begin.toString + ")", " end: ", timeformater.format(end) + " (" + end.toString + ")")

    val t = ctx.timestamp()

    println("curWaterMark", t, timeformater.format(t))
    println()

    try {
      if(!alertUseMapper.contains(alertUseKey)) {
        // println("Registered AlertUse: ", begin, end)
        println("Fresh alertUseMapper: ", alertUseKey)
        println("========================================")
        println()
        alertUseMapper.put(alertUseKey, (false, begin, end, alertUse.Cap))
        //
        //      var current = ctx.timerService().currentWatermark()
        //        current = current + (1000 - (current % 1000))

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
    println("======================================================================")
    println("onTimer:")
  }

  @transient private var checkpointedState: ListState[(Int, Int, Int, Boolean, Long, Long, Int)] = null

  override def initializeState(initCtx: FunctionInitializationContext): Unit = {
    println("alertUseMapper initializeState")
    alertUseMapper = initCtx.getKeyedStateStore.getMapState(keyAlertUseDescriptor)

    lazy val descriptor = new ListStateDescriptor[(Int, Int, Int, Boolean, Long, Long, Int)] ("alertUserMap",  createTypeInformation[(Int, Int, Int, Boolean, Long, Long, Int)])

    val backendStateAlertList = initCtx.getOperatorStateStore.getListState(descriptor)

    println("initCtx.isRestored", initCtx.isRestored)

    if (initCtx.isRestored) {
      val iterator = backendStateAlertList.get().iterator()
      alertUseMapper.clear()
      while(iterator.hasNext) {
        val item = iterator.next()
        println("LOADING SNAPSOT: ", item)
        val alertUseKey : (Int, Int, Int) = (item._1, item._2, item._3)
        val alertUseValue : (Boolean, Long, Long, Int) = (item._4, item._5, item._6, item._7)
        alertUseMapper.put(alertUseKey, alertUseValue)
      }
    }
  }

  override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
    println("saving alertUseMapper snapshotState")
    checkpointedState.clear()
    println("saving", alertUseMapper.keys())
    val mapIterator = alertUseMapper.iterator()
    while(mapIterator.hasNext) {
      val item = mapIterator.next()
      val key = item.getKey
      val value = item.getValue
      checkpointedState.add((key._1, key._2, key._3, value._1, value._2, value._3, value._4))
    }
    println("checkpointedState", checkpointedState)
    println("saving checkingpoint", ctx.getCheckpointId, ctx.getCheckpointTimestamp)
  }
}