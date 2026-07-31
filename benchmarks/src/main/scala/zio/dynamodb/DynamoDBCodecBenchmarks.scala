/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import dynosaur.Schema.WriteError
import dynosaur.{ DynamoValue => DynosaurValue }
import org.openjdk.jmh.annotations._
import org.scanamo.DynamoReadError.describe
import org.scanamo.{ DynamoValue => ScanamoValue }
import zio.blocks.schema.Schema
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.schema.{ DeriveSchema, Schema => ZIOSchema }

/**
 * Codec encode/decode throughput comparison across four libraries:
 * zio-blocks (this library), zio-schema, Dynosaur, and Scanamo.
 *
 * Measures `reading` (decode) and `writing` (encode) of a `List[Person]`
 * of varying length, exercising each library's codec on the same data.
 *
 * Borrows from Andriy Plokhotnyuk's zio-blocks benchmarks
 * https://github.com/zio/zio-blocks
 *
 * ==Run all benchmarks==
 * {{{
 * sbt "benchmarks/jmh:run DynamoDBCodecBenchmarks"
 * }}}
 *
 * ==Run with allocation profiling==
 * {{{
 * sbt "benchmarks/jmh:run -prof gc DynamoDBCodecBenchmarks"
 * }}}
 *
 * ==Run a single benchmark method==
 * {{{
 * sbt "benchmarks/jmh:run DynamoDBCodecBenchmarks.readingZioBlocks"
 * }}}
 */
class DynamoDBCodecBenchmarks extends BaseBenchmark {
  import BenchmarkDomain._

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int                                              = 1
  var listOfRecords: List[Person]                            = _
  var encodedListOfRecordsForBlocks: List[AttributeValue]    = _
  var encodedListOfRecordsForZioSchema: List[AttributeValue] = _
  var encodedListOfRecordsForDynosaur: List[DynosaurValue]   = _
  var encodedListOfRecordsForScanamo: List[ScanamoValue]     = _

  @Setup
  def setup(): Unit = {
    listOfRecords = (1 to size)
      .map(_ =>
        Person(
          12345678901L,
          "John",
          30,
          Some("123 Main St")
//          Map("a" -> 1, "b" -> 2, "c" -> 3),
//          Vector(1, 2, 3, 4, 5),
//          (1, 2L, "3"),
//          TrafficLight.Green,
//          PaymentMethod.DebitCard("123", "123")
        )
      )
      .toList

    encodedListOfRecordsForBlocks = listOfRecords.map(zioBlocksCodec.encoder(_))
    encodedListOfRecordsForZioSchema = listOfRecords.map(zioSchemaEncoder(_))
    encodedListOfRecordsForDynosaur = listOfRecords.map { x =>
      DynosaurSchema.personSchema.write(x).getOrElse(throw new Exception("Failed to encode"))
    }
    encodedListOfRecordsForScanamo = listOfRecords.map { x =>
      ScanamoCodec.person.write(x)
    }
  }

  @Benchmark
  def readingScanamo: List[Person] =
    encodedListOfRecordsForScanamo.map(av =>
      ScanamoCodec.person.read(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(describe(error))
      }
    )

  @Benchmark
  def readingDynosaur: List[Person] =
    encodedListOfRecordsForDynosaur.map(av =>
      DynosaurSchema.personSchema.read(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def readingZioSchema: List[Person] =
    encodedListOfRecordsForZioSchema.map(av =>
      zioSchemaDecoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def readingZioBlocks: List[Person] =
    encodedListOfRecordsForBlocks.map { av =>
      zioBlocksCodec.decoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    }

  @Benchmark
  def writingScanamo: Seq[ScanamoValue] = listOfRecords.map(ScanamoCodec.person.write)

  @Benchmark
  def writingDynosaur: Seq[Either[WriteError, DynosaurValue]] = listOfRecords.map(DynosaurSchema.personSchema.write)

  @Benchmark
  def writingZioSchema: Seq[AttributeValue] = listOfRecords.map(zioSchemaEncoder(_))

  @Benchmark
  def writingZioBlocks: Seq[AttributeValue] = listOfRecords.map(zioBlocksCodec.encoder(_))

}

object BenchmarkDomain {
  sealed trait PaymentMethod
  object PaymentMethod {
    final case class CreditCard(number: String, cvv: String) extends PaymentMethod
    object CreditCard {
      implicit val schema: Schema[CreditCard]       = Schema.derived
      implicit val zioSchema: ZIOSchema[CreditCard] = DeriveSchema.gen[CreditCard]
    }
    final case class DebitCard(number: String, cvv: String) extends PaymentMethod
    object DebitCard  {
      implicit val schema: Schema[DebitCard]       = Schema.derived
      implicit val zioSchema: ZIOSchema[DebitCard] = DeriveSchema.gen[DebitCard]
    }
    final case class PayPal(email: String) extends PaymentMethod
    object PayPal     {
      implicit val schema: Schema[PayPal]       = Schema.derived
      implicit val zioSchema: ZIOSchema[PayPal] = DeriveSchema.gen[PayPal]
    }

    implicit val schema: Schema[PaymentMethod]       = Schema.derived
    implicit val zioSchema: ZIOSchema[PaymentMethod] = DeriveSchema.gen[PaymentMethod]
  }

  sealed trait TrafficLight
  object TrafficLight {
    case object Red    extends TrafficLight
    case object Yellow extends TrafficLight
    case object Green  extends TrafficLight

    implicit val schema: Schema[TrafficLight] = Schema.derived
  }
  case class Person(
    id: Long,
    name: String,
    age: Int,
    address: Option[String]
//    map: Map[String, Int],
//    list: Vector[Int],
//    tuple: (Int, Long, String),
//    light: TrafficLight,
//    paymentMethod: PaymentMethod
  )
  object Person       {
    implicit val blocksSchema: Schema[Person] = Schema.derived
  }
  val zioSchema: ZIOSchema[Person] = DeriveSchema.gen[Person]

  val zioSchemaEncoder: Encoder[Person] = Codec.encoder[Person](zioSchema)
  val zioSchemaDecoder: Decoder[Person] = Codec.decoder[Person](zioSchema)

  val zioBlocksCodec: DynamoDBCodec[Person] = Schema.derived.deriving(DynamoDBCodecDeriver).derive
}
