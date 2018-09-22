package com.getcake.mappers

import com.getcake.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class TrafficAlertFilterFunction extends CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)] {
  //Map ClientID entity_id AlertUseId

  lazy val alertUseMapper: MapState[(Int, Int, Int), (Boolean, Long, Long)] =
    getRuntimeContext.getMapState(
    new MapStateDescriptor[(Int, Int, Int), (Boolean, Long, Long)]("alertUseMapper", createTypeInformation[(Int, Int, Int)], createTypeInformation[(Boolean, Long, Long)])
  )

  lazy val timeformater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sssZ")

  override def processElement1(
    streamData: StreamData,
    ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)]#Context,
    out: Collector[(String, Int, Int, Int, Long, Long)]): Unit = {

    val publisherKey : (Int, Int, Int) = (streamData.client_id, streamData.publisher_id.getOrElse(0), 1)
    val offerKey : (Int, Int, Int) = (streamData.client_id, streamData.offer_id.getOrElse(0), 2)
    val campaignKey : (Int, Int, Int) = (streamData.client_id, streamData.campaign_id.getOrElse(0), 3)

    // check if we may forward the reading
    if(alertUseMapper.contains(publisherKey)) {
      val hasPublisher = alertUseMapper.get(publisherKey)
      out.collect(("filteredData", streamData.client_id, streamData.publisher_id.getOrElse(0), 1, hasPublisher._2, hasPublisher._3))
    }

    if(alertUseMapper.contains(offerKey)) {
      val hasOffer = alertUseMapper.get(offerKey)
      out.collect(("filteredData", streamData.client_id, streamData.offer_id.getOrElse(0), 2, hasOffer._2, hasOffer._3))
    }

    if(alertUseMapper.contains(campaignKey)) {
      val hasCampaign = alertUseMapper.get(campaignKey)
      out.collect(("filteredData", streamData.client_id, streamData.campaign_id.getOrElse(0), 3, hasCampaign._2, hasCampaign._3))
    }
  }

  override def processElement2(
      alertUse: AlertUse,
      ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)]#Context,
      out: Collector[(String, Int, Int, Int, Long, Long)]): Unit = {

    // set disable forward timer
    val alertUseKey : (Int, Int, Int) = (alertUse.ClientID, alertUse.EntityID, alertUse.EntityTypeID)
      val begin = timeformater.parse(alertUse.AlertUseBegin).getTime
      val end = timeformater.parse(alertUse.AlertUseEnd).getTime

    if(!alertUseMapper.contains(alertUseKey)) {
      // println("Registered AlertUse: ", begin, end)
      alertUseMapper.put(alertUseKey, (true, begin, end))

      var current = ctx.timerService().currentWatermark()
      current = current + (1000 - (current % 1000))
      ctx.timerService().registerEventTimeTimer(end+5000)
      println("processElement2 ", "begin: ", begin, "end: ", end, "timenow: ", timeformater.format(current), current)
      }
  }

  override def onTimer(
      ts: Long,
      ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)]#OnTimerContext,
      out: Collector[(String, Int, Int, Int, Long, Long)]): Unit = {

      println("onTimer ", timeformater.format(ctx.timerService().currentProcessingTime()), ts)
  }
}