package com.getcake

case class StreamEvent(input: Array[Byte]) {
  var clientID : String = input.toString
}
