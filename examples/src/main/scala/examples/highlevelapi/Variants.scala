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
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbUpdateExpr }
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * Sealed-trait fields: `Status` (all cases field-less, stored as a plain string) versus
 * `Shape` (mixed field/no-field cases, stored as a discriminated map) — and the
 * `discriminatorKind` options a `Table` can pick for the latter. `.when[Case]` optics narrow
 * a field down to one case.
 */
object Variants {

  sealed trait Status
  object Status {
    case object Pending   extends Status
    case object Shipped   extends Status
    case object Cancelled extends Status
    implicit val schema: Schema[Status] = Schema.derived
  }

  sealed trait Shape
  object Shape {
    case object NoShape                     extends Shape
    final case class Circle(radius: Double) extends Shape
    final case class Square(side: Double)   extends Shape
    implicit val schema: Schema[Shape] = Schema.derived
  }

  final case class Item(id: String, status: Status, shape: Shape)

  object Item extends CompanionOptics[Item] {
    implicit val schema: Schema[Item] = Schema.derived

    val id: Lens[Item, String]               = $(_.id)
    val status: Lens[Item, Status]           = $(_.status)
    val shape: Lens[Item, Shape]             = $(_.shape)
    val circleRadius: Optional[Item, Double] = $(_.shape.when[Shape.Circle].radius)
    val squareSide: Optional[Item, Double]   = $(_.shape.when[Shape.Square].side)
  }

  val items: Table[Item] = Table[Item]("items")

  // Status — all-no-field cases: encodes as a bare AttributeValue.String, no discriminator
  // wrapper regardless of discriminatorKind (there's no per-case data to disambiguate).
  val statusEq: DdbExpr[Item, Boolean] = Item.status === Status.Shipped
  val setStatus: DdbUpdateExpr[Item]   = Item.status.set(Status.Cancelled)

  // Shape — mixed field/no-field cases, default Key discriminator: a discriminated Map.
  val isCircle: DdbExpr[Item, Boolean]       = Item.circleRadius.attributeExists
  val circleRadiusGt: DdbExpr[Item, Boolean] = Item.circleRadius > 0.0
  val setCircleRadius: DdbUpdateExpr[Item]   = Item.circleRadius.set(5.0)
  val isNoShapeCase: DdbExpr[Item, Boolean]  = Item.shape === Shape.NoShape

  // Same Item, but the case is stored under a sibling "type" field instead of as the map key.
  val itemsFieldDiscriminated: Table[Item] =
    Table[Item]("items").deriving(_.withDiscriminatorKind(DiscriminatorKind.Field("type")))

  // DiscriminatorKind.None compiles, but a `.when[Case]` path can't be used against a Table
  // configured this way — it fails when the query runs, not at compile time.
  val itemsNoDiscriminator: Table[Item] =
    Table[Item]("items").deriving(_.withDiscriminatorKind(DiscriminatorKind.None))

  // Wired into the six CRUD operations — see VariantsSpec for these actually run.
  val putQuery: DynamoDBQuery[Item, Option[Item]] =
    put(items, Item("i-1", Status.Pending, Shape.Circle(1.0)))

  val getQuery: DynamoDBQuery[Item, Either[DynamoDBError.ItemError, Item]] =
    get(items)(Item.id.partitionKey === "i-1")

  val updateQuery: DynamoDBQuery[Item, Option[Item]] =
    update(items)(Item.id.partitionKey === "i-1")(setStatus)

  val deleteQuery: DynamoDBQuery[Item, Option[Item]] =
    deleteFrom(items)(Item.id.partitionKey === "i-1")

  val queryQuery: DynamoDBQuery[Item, Page[Either[DynamoDBError.ItemError, Item]]] =
    query(items, limit = 20).whereKey(Item.id.partitionKey === "i-1").filter(statusEq)

  val scanQuery: DynamoDBQuery[Item, Page[Either[DynamoDBError.ItemError, Item]]] =
    scan(items, limit = 20).filter(isCircle)
}
