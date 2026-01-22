package zio.dynamodb.benchmarks.codecs

import org.scanamo._
import org.scanamo.generic.semiauto._
import BenchmarkDomain._

object ScanamoCodec {
  implicit val paymentMethod: DynamoFormat[PaymentMethod] = deriveDynamoFormat
  implicit val trafficLight: DynamoFormat[TrafficLight]   = deriveDynamoFormat
  implicit val tuple: DynamoFormat[(Int, Long, String)]   = deriveDynamoFormat
  implicit val person: DynamoFormat[Person]               = deriveDynamoFormat
}
