package com.getcake.mappers

import com.getcake.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import java.text.SimpleDateFormat

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

class TrafficAlertFilterFunction(useCheckPoint: Boolean) extends CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)] with CheckpointedFunction {
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
    ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)]#Context,
    out: Collector[(String, Int, Int, Int, Long, Long)]): Unit = {

    waitForloadingCheckPoint()

    val publisherKey : (Int, Int, Int) = (streamData.client_id, streamData.publisher_id.getOrElse(0), 1)
    val offerKey : (Int, Int, Int) = (streamData.client_id, streamData.offer_id.getOrElse(0), 2)
    val campaignKey : (Int, Int, Int) = (streamData.client_id, streamData.campaign_id.getOrElse(0), 3)

    //println(ctx.timestamp(), ctx.timerService().currentWatermark(), ctx.timerService().currentProcessingTime())
    // check if we may forward the reading
    val now = ctx.timestamp()
    if(alertUseMapper.contains(publisherKey)) {
      val hasPublisher = alertUseMapper.get(publisherKey)
      if(hasPublisher._2 <= now && now <= hasPublisher._3)
        out.collect(("filteredData", streamData.client_id, streamData.publisher_id.getOrElse(0), 1, hasPublisher._2, hasPublisher._3))
    }

    if(alertUseMapper.contains(offerKey)) {
      val hasOffer = alertUseMapper.get(offerKey)
      if(hasOffer._2 <= now && now <= hasOffer._3)
        out.collect(("filteredData", streamData.client_id, streamData.offer_id.getOrElse(0), 2, hasOffer._2, hasOffer._3))
    }
//
    if(alertUseMapper.contains(campaignKey)) {
      val hasCampaign = alertUseMapper.get(campaignKey)
      if(hasCampaign._2 <= now && now <= hasCampaign._3)
        out.collect(("filteredData", streamData.client_id, streamData.campaign_id.getOrElse(0), 3, hasCampaign._2, hasCampaign._3))
    }
  }

  override def processElement2(
      alertUse: AlertUse,
      ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)]#Context,
      out: Collector[(String, Int, Int, Int, Long, Long)]): Unit = {

    waitForloadingCheckPoint()
    // set disable forward timer
    val alertUseKey : (Int, Int, Int) = (alertUse.ClientID, alertUse.EntityID, alertUse.EntityTypeID)
    val begin = timeformater.parse(alertUse.AlertUseBegin).getTime
    val end = timeformater.parse(alertUse.AlertUseEnd).getTime

    try {
      if(!alertUseMapper.contains(alertUseKey)) {
        // println("Registered AlertUse: ", begin, end)
        println("Fresh alertUseMapper: ", alertUseKey)
        alertUseMapper.put(alertUseKey, (false, begin, end, 0))
        //
        //      var current = ctx.timerService().currentWatermark()
        //        current = current + (1000 - (current % 1000))
        ctx.timerService().registerEventTimeTimer(end) // invoke endtime
        println("processElement2 ", " client_id: ",alertUseKey._1, "entity_id: ", alertUseKey._2 , "entity_type_id: ", alertUseKey._3)
        println("begin: ", begin, timeformater.format(begin), " end: ", end, timeformater.format(end))
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
      ctx: CoProcessFunction[StreamData, AlertUse, (String, Int, Int, Int, Long, Long)]#OnTimerContext,
      out: Collector[(String, Int, Int, Int, Long, Long)]): Unit = {

      //remove list from alertUseMapper
      val mapIterator = alertUseMapper.iterator()
      while(mapIterator.hasNext) {
          val item = mapIterator.next()
          if(item.getValue._3 == ts) //if endtime has arrive{
          {
            println("onTimer ", "watermark: ", timeformater.format(ctx.timerService().currentWatermark()), "processTime: ", timeformater.format(ctx.timerService().currentProcessingTime()), "registerTime: ", timeformater.format(ts))
            val originValue = item.getValue
            val key = item.getKey
            println("onTimer before Reset: ", originValue)
            alertUseMapper.put(key, (false, originValue._2, originValue._3, 0))
            println("onTimer Reset: ", key)
          }
      }
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

  override def initializeState(initCtx: FunctionInitializationContext): Unit = {
    this.alertUseMapperCheckPoint = initCtx.getKeyedStateStore.getMapState(keyAlertUseDescriptor)
  }

  override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
    val mapIterator = alertUseMapper.iterator()
    while(mapIterator.hasNext) {
      val item = mapIterator.next()
      this.alertUseMapperCheckPoint.put(item.getKey, item.getValue)
    }
    println("saving checkingpoint", ctx.getCheckpointId, ctx.getCheckpointTimestamp)
  }
}