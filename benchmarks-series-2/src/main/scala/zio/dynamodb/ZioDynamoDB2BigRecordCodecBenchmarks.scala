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

import scala.compiletime.uninitialized

import org.openjdk.jmh.annotations.{ Scope => JScope, _ }
import zio.schema.{ DeriveSchema, Schema }

import java.util.concurrent.TimeUnit

/**
 * The zio-dynamodb 2.x side of `BigRecordCodecBenchmarks`: the same `BigRecord` data, `size`s and JMH
 * settings.
 *
 * {{{
 * sbt "benchmarksSeries2/Jmh/run -prof gc ZioDynamoDB2BigRecordCodecBenchmarks"
 * }}}
 */
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
class ZioDynamoDB2BigRecordCodecBenchmarks {
  import ZioDynamoDB2BigRecordCodecBenchmarks._

  @Param(Array("1", "1000"))
  var size: Int                            = 1
  var records: List[BigRecord]             = uninitialized
  var encodedRecords: List[AttributeValue] = uninitialized

  @Setup
  def setup(): Unit = {
    records = List.fill(size)(BigRecord.sample)
    encodedRecords = records.map(encoder(_))
    require(decoder(encodedRecords.head) == Right(BigRecord.sample), "zio-dynamodb 2.x round trip")
  }

  @Benchmark
  def readingZioSchema: List[BigRecord] =
    encodedRecords.map(av =>
      decoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def writingZioSchema: Seq[AttributeValue] = records.map(encoder(_))
}

object ZioDynamoDB2BigRecordCodecBenchmarks {

  sealed trait Priority
  object Priority {
    case object Low      extends Priority
    case object Medium   extends Priority
    case object High     extends Priority
    case object Critical extends Priority
    implicit val schema: Schema[Priority] = DeriveSchema.gen
  }

  sealed trait Fulfilment
  object Fulfilment {
    final case class Warehouse(code: String)                 extends Fulfilment
    final case class DropShip(vendor: String, leadDays: Int) extends Fulfilment
    case object Pickup                                       extends Fulfilment
    implicit val schema: Schema[Fulfilment] = DeriveSchema.gen
  }

  sealed trait Contact
  object Contact {
    final case class Email(address: String)                  extends Contact
    final case class Phone(number: String, ext: Option[Int]) extends Contact
    implicit val schema: Schema[Contact] = DeriveSchema.gen
  }

  final case class Address(street: String, city: String, state: String, zip: String, country: String)
  object Address { implicit val schema: Schema[Address] = DeriveSchema.gen }

  final case class GeoPoint(lat: Double, lng: Double)
  object GeoPoint { implicit val schema: Schema[GeoPoint] = DeriveSchema.gen }

  final case class AuditInfo(createdBy: String, createdAt: Long, updatedBy: String, updatedAt: Long, version: Int)
  object AuditInfo { implicit val schema: Schema[AuditInfo] = DeriveSchema.gen }

  final case class Money(currency: String, amountMinor: Long)
  object Money { implicit val schema: Schema[Money] = DeriveSchema.gen }

  final case class Dimensions(widthMm: Int, heightMm: Int, depthMm: Int, weightG: Long)
  object Dimensions { implicit val schema: Schema[Dimensions] = DeriveSchema.gen }

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
    implicit val schema: Schema[BigRecord] = DeriveSchema.gen

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

  val encoder: Encoder[BigRecord] = Codec.encoder[BigRecord](BigRecord.schema)
  val decoder: Decoder[BigRecord] = Codec.decoder[BigRecord](BigRecord.schema)
}
