package com.getcake.sourcetype

case class AlertUse (
    ClientID: Int,
    AlertUseID: Int,
    EntityTypeID: Int,
    EntityID: Int,
    AlertUseTypeID: Int,
    AlertUseInterval: Long,
    AlertUseBegin: String,
    AlertUseEnd: String,
    Cap: Int)
