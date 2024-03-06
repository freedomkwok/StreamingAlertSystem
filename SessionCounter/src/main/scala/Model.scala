import org.apache.avro.reflect.AvroSchema

object Model {
  case class ServerHeartbeat (
     instanceId: String,
    serverType: String,
    productId: String,
    customerId: String,
    timestamp: Long
  ) extends Serializable

  case class SessionOnInstanceLevel (
     id: String,
     productId: String,
     totalTime: Long,
     timestamp: Long
   )

  case class SessionOnProductLevel (
      productId: String,
      totalTime: Long,
      timestamp: Long,
      watermarkTime: Long
  )

  case class SessionOnCustomerLevel (
     id: String,
     totalTime: Long
    )
}
