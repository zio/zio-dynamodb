package zio.dynamodb.benchmarks.codecs

import org.scanamo._
import org.scanamo.generic.semiauto._
import BenchmarkDomain._

object ScanamoCodec {
  implicit val person: DynamoFormat[Person] = deriveDynamoFormat
}
