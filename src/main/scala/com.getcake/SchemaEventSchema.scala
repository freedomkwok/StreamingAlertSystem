package com.getcake

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.streaming.api.scala._

class SchemaEventSchema extends DeserializationSchema[StreamEvent] with SerializationSchema[StreamEvent] {
  override def serialize (element: StreamEvent): Array[Byte] = {
    element.toString.getBytes
  }

  override def deserialize (message: Array[Byte] ): StreamEvent = {
    StreamEvent(message)
  }

  override def isEndOfStream (nextElement: StreamEvent): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[StreamEvent] = {
    createTypeInformation[StreamEvent]
  }
}
