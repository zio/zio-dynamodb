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

package examples.highlevelapi

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbUpdateExpr }
import zio.dynamodb.blocks.ddbexpr.dsl.*

/**
 * Scala 3 `enum` types work with `Schema` derivation the same way sealed traits do (see
 * `Variants.scala`): an all-no-field enum and one with data-carrying cases.
 */
object Enums {

  enum Status derives Schema {
    case Pending, Shipped, Cancelled
  }

  enum Shape derives Schema {
    case NoShape
    case Circle(radius: Double)
    case Square(side: Double)
  }

  final case class Item(id: String, status: Status, shape: Shape) derives Schema

  object Item extends CompanionOptics[Item] {
    val id: Lens[Item, String]               = $(_.id)
    val status: Lens[Item, Status]           = $(_.status)
    val shape: Lens[Item, Shape]             = $(_.shape)
    val circleRadius: Optional[Item, Double] = $(_.shape.when[Shape.Circle].radius)
  }

  val items: Table[Item] = Table[Item]("items")

  // All-no-field enum — stored as a plain string.
  val statusEq: DdbExpr[Item, Boolean] = Item.status === Status.Shipped
  val setStatus: DdbUpdateExpr[Item]   = Item.status.set(Status.Cancelled)

  // A data-carrying case, narrowed with .when[Case] same as a sealed-trait Variant.
  val isCircle: DdbExpr[Item, Boolean]      = Item.circleRadius.attributeExists
  val setCircleRadius: DdbUpdateExpr[Item]  = Item.circleRadius.set(5.0)
  val isNoShapeCase: DdbExpr[Item, Boolean] = Item.shape === Shape.NoShape
}
