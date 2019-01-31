package com.ngenda

case class StreamEvent(input: Array[Byte]) {
  var clientID : String = input.toString
}
