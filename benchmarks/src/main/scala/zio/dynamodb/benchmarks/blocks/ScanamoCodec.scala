package zio.dynamodb.benchmarks.blocks

import org.scanamo._
import org.scanamo.generic.semiauto._
import ListOfRecordsDomain._

object ScanamoCodec {
  implicit val paymentMethod: DynamoFormat[PaymentMethod] = deriveDynamoFormat
  implicit val person: DynamoFormat[Person]               = deriveDynamoFormat
}
