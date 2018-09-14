package com.getcake.sourcetype

case class StreamData (click_id: Option[String],
                        client_id: Int,
                        request_date_utc: Long,
                        request_date: Long,
                        campaign_id: Option[Int],
                        publisher_id: Option[Int])
