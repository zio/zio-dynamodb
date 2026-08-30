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

  // ── Large, nested, sum-typed record ──────────────────────────────────────
  // ~26 top-level fields, 5 distinct nested case-class types, 3 sum types
  // (a pure enumeration, a mixed data/object union, and a data-carrying union),
  // Options, List, Vector and Map. Used by CeEffectStackBench to compare get/put
  // on a realistic payload rather than the flat 4-field Person.

  sealed trait Priority
  object Priority {
    case object Low      extends Priority
    case object Medium   extends Priority
    case object High     extends Priority
    case object Critical extends Priority
    implicit val schema: Schema[Priority] = Schema.derived
  }

  sealed trait Fulfilment
  object Fulfilment {
    final case class Warehouse(code: String)                 extends Fulfilment
    final case class DropShip(vendor: String, leadDays: Int) extends Fulfilment
    case object Pickup                                       extends Fulfilment
    implicit val schema: Schema[Fulfilment] = Schema.derived
  }

  sealed trait Contact
  object Contact {
    final case class Email(address: String)                  extends Contact
    final case class Phone(number: String, ext: Option[Int]) extends Contact
    implicit val schema: Schema[Contact] = Schema.derived
  }

  final case class Address(street: String, city: String, state: String, zip: String, country: String)
  object Address { implicit val schema: Schema[Address] = Schema.derived }

  final case class GeoPoint(lat: Double, lng: Double)
  object GeoPoint { implicit val schema: Schema[GeoPoint] = Schema.derived }

  final case class AuditInfo(createdBy: String, createdAt: Long, updatedBy: String, updatedAt: Long, version: Int)
  object AuditInfo { implicit val schema: Schema[AuditInfo] = Schema.derived }

  final case class Money(currency: String, amountMinor: Long)
  object Money { implicit val schema: Schema[Money] = Schema.derived }

  final case class Dimensions(widthMm: Int, heightMm: Int, depthMm: Int, weightG: Long)
  object Dimensions { implicit val schema: Schema[Dimensions] = Schema.derived }

  final case class BigRecord(
    id: Long,
    sku: String,
    name: String,
    description: String,
    category: String,
    subCategory: String,
    brand: String,
    priority: Priority,
    fulfilment: Fulfilment,
    primaryContact: Contact,
    secondaryContact: Option[Contact],
    billingAddress: Address,
    shippingAddress: Option[Address],
    location: GeoPoint,
    audit: AuditInfo,
    price: Money,
    cost: Money,
    dimensions: Dimensions,
    tags: List[String],
    ratings: Vector[Int],
    attributes: Map[String, String],
    quantityOnHand: Int,
    reorderPoint: Int,
    discontinued: Boolean,
    averageRating: Double,
    notes: Option[String]
  )
  object BigRecord {
    implicit val blocksSchema: Schema[BigRecord] = Schema.derived

    val sample: BigRecord = BigRecord(
      id = 999000111222L,
      sku = "SKU-ABC-12345",
      name = "Widget Assembly, Deluxe",
      description = "A deluxe widget assembly with reinforced housing and extended warranty.",
      category = "Hardware",
      subCategory = "Assemblies",
      brand = "Acme",
      priority = Priority.High,
      fulfilment = Fulfilment.DropShip("vendor-42", leadDays = 5),
      primaryContact = Contact.Email("sales@example.com"),
      secondaryContact = Some(Contact.Phone("+1-555-0100", ext = Some(220))),
      billingAddress = Address("1 Market St", "San Francisco", "CA", "94105", "US"),
      shippingAddress = Some(Address("500 Terry A Francois Blvd", "San Francisco", "CA", "94158", "US")),
      location = GeoPoint(37.7749, -122.4194),
      audit = AuditInfo("import-job", 1_724_000_000_000L, "ops-user", 1_724_900_000_000L, version = 7),
      price = Money("USD", amountMinor = 129_99L),
      cost = Money("USD", amountMinor = 74_50L),
      dimensions = Dimensions(widthMm = 320, heightMm = 180, depthMm = 95, weightG = 2_400L),
      tags = List("featured", "clearance", "bulk-eligible"),
      ratings = Vector(5, 4, 5, 3, 5, 4),
      attributes = Map("colour" -> "graphite", "material" -> "aluminium", "finish" -> "matte"),
      quantityOnHand = 1_284,
      reorderPoint = 200,
      discontinued = false,
      averageRating = 4.33,
      notes = Some("Palletised; 24 units per pallet.")
    )
  }
}
