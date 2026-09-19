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

package examples.hlapi

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbUpdateExpr }
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * A three-level nested record path (`Order.shipping.geo.lat`), in both a condition and an
 * update expression.
 */
object NestedRecords {

  final case class GeoPoint(lat: Double, lng: Double)

  object GeoPoint extends CompanionOptics[GeoPoint] {
    implicit val schema: Schema[GeoPoint] = Schema.derived

    val lat: Lens[GeoPoint, Double] = $(_.lat)
    val lng: Lens[GeoPoint, Double] = $(_.lng)
  }

  final case class Address(street: String, geo: GeoPoint)

  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived

    val street: Lens[Address, String] = $(_.street)
    val geo: Lens[Address, GeoPoint]  = $(_.geo)
  }

  final case class Order(id: String, shipping: Address)

  object Order extends CompanionOptics[Order] {
    implicit val schema: Schema[Order] = Schema.derived

    val id: Lens[Order, String]          = $(_.id)
    val shipping: Lens[Order, Address]   = $(_.shipping)
    val shippingLat: Lens[Order, Double] = $(_.shipping.geo.lat)
    val shippingLng: Lens[Order, Double] = $(_.shipping.geo.lng)
  }

  val orders: Table[Order] = Table[Order]("orders")

  val latGt: DdbExpr[Order, Boolean]             = Order.shippingLat > 0.0
  val inBothHemispheres: DdbExpr[Order, Boolean] = Order.shippingLat > 0.0 && Order.shippingLng < 0.0
  val setLat: DdbUpdateExpr[Order]               = Order.shippingLat.set(51.5)
  val setBoth: DdbUpdateExpr[Order]              = Order.shippingLat.set(51.5) + Order.shippingLng.set(-0.1)
}
