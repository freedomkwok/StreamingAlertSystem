package com.getcake.mappers

import com.getcake.StreamEvent
import com.getcake.sourcetype.{AlertUse, StreamData}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.scala.typeutils._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class TrafficAlertFilterFunction extends CoProcessFunction[StreamData, AlertUse, Tuple5[String, Int, Int, Int, Int]] {
  //Map ClientID entity_id AlertUseId
  lazy val alertUseMapper: MapState[Tuple3[Int, Int, Int], Boolean] =
    getRuntimeContext.getMapState(
    new MapStateDescriptor[Tuple3[Int, Int, Int], Boolean]("alertUseMapper", createTypeInformation[Tuple3[Int, Int, Int]], Types.of[Boolean])
  )
  lazy val counter : Int = 1

  override def processElement1(
    streamData: StreamData,
    ctx: CoProcessFunction[StreamData, AlertUse, Tuple5[String, Int, Int, Int, Int]]#Context,
    out: Collector[Tuple5[String, Int, Int, Int, Int]]): Unit = {

    val publisherKey : Tuple3[Int, Int, Int] = new Tuple3(streamData.client_id, streamData.publisher_id.getOrElse(0), 1)
    val offerKey : Tuple3[Int, Int, Int] = new Tuple3(streamData.client_id, streamData.offer_id.getOrElse(0), 2)
    val campaignKey : Tuple3[Int, Int, Int] = new Tuple3(streamData.client_id, streamData.campaign_id.getOrElse(0), 3)

    // check if we may forward the reading

    if(alertUseMapper.contains(publisherKey) || alertUseMapper.contains(offerKey) || alertUseMapper.contains(campaignKey)) {
      out.collect(new Tuple5("filteredData", streamData.client_id, streamData.publisher_id.getOrElse(0), streamData.offer_id.getOrElse(0), streamData.campaign_id.getOrElse(0)))
    }
  }

  override def processElement2( alertUse: AlertUse,
                                ctx: CoProcessFunction[StreamData, AlertUse, Tuple5[String, Int, Int, Int, Int]]#Context,
                                out: Collector[Tuple5[String, Int, Int, Int, Int]]): Unit = {

    // set disable forward timer
    val alertUseKey : Tuple3[Int, Int, Int] = new Tuple3(alertUse.ClientID, alertUse.EntityID, alertUse.AlertUseID)
    if(!alertUseMapper.contains(alertUseKey)) {
      alertUseMapper.put(alertUseKey, true)
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())
    }
  }

  override def onTimer( ts: Long,
                        ctx: CoProcessFunction[StreamData, AlertUse, Tuple5[String, Int, Int, Int, Int]]#OnTimerContext,
                        out: Collector[Tuple5[String, Int, Int, Int, Int]]): Unit = {

  }
}