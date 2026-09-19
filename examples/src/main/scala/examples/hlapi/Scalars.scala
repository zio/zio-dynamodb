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

import zio.blocks.chunk.Chunk
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.DdbExpr
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * Relational and range operators (`===`, `>`, `<`, `>=`, `<=`, `!=`, `between`, `in`,
 * `inSet`) against DynamoDB's scalar attribute types: numbers, strings, booleans, and byte
 * sequences (`Chunk[Byte]`).
 */
object Scalars {

  final case class Widget(id: String, qty: Int, price: Double, active: Boolean, blob: Chunk[Byte])

  object Widget extends CompanionOptics[Widget] {
    implicit val schema: Schema[Widget] = Schema.derived

    val id: Lens[Widget, String]        = $(_.id)
    val qty: Lens[Widget, Int]          = $(_.qty)
    val price: Lens[Widget, Double]     = $(_.price)
    val active: Lens[Widget, Boolean]   = $(_.active)
    val blob: Lens[Widget, Chunk[Byte]] = $(_.blob)
  }

  val widgets: Table[Widget] = Table[Widget]("widgets")

  // S — relational + range
  val sEq: DdbExpr[Widget, Boolean]      = Widget.id === "w-1"
  val sNeq: DdbExpr[Widget, Boolean]     = Widget.id != "w-1"
  val sLt: DdbExpr[Widget, Boolean]      = Widget.id < "w-9"
  val sBetween: DdbExpr[Widget, Boolean] = Widget.id.between("w-1", "w-9")
  val sIn: DdbExpr[Widget, Boolean]      = Widget.id.in("w-1", "w-2", "w-3")
  val sInSet: DdbExpr[Widget, Boolean]   = Widget.id.inSet(Set("w-1", "w-2", "w-3")) // same as .in, Set literal

  // N — relational + range
  val nEq: DdbExpr[Widget, Boolean]      = Widget.qty === 10
  val nGte: DdbExpr[Widget, Boolean]     = Widget.qty >= 1
  val nLte: DdbExpr[Widget, Boolean]     = Widget.price <= 99.99
  val nBetween: DdbExpr[Widget, Boolean] = Widget.qty.between(1, 100)
  val nIn: DdbExpr[Widget, Boolean]      = Widget.qty.in(1, 2, 3)

  // BOOL — equality only; range/between/in are meaningless for a two-valued type and the
  // library doesn't expose them for it
  val boolEq: DdbExpr[Widget, Boolean] = Widget.active === true

  // B — relational + range (byte sequences compare lexicographically on the wire)
  val bEq: DdbExpr[Widget, Boolean]      = Widget.blob === Chunk[Byte](1, 2, 3)
  val bBetween: DdbExpr[Widget, Boolean] = Widget.blob.between(Chunk[Byte](0), Chunk[Byte](-1))

  // Wired into the six CRUD operations — see ScalarsSpec for these actually run. Each
  // builder converts to a DynamoDBQuery implicitly, so no `.toQuery` call is needed.
  val putQuery: DynamoDBQuery[Widget, Option[Widget]] =
    put(widgets, Widget("w-1", 10, 9.99, active = true, Chunk[Byte](1, 2, 3)))

  val getQuery: DynamoDBQuery[Widget, Either[DynamoDBError.ItemError, Widget]] =
    get(widgets)(Widget.id.partitionKey === "w-1")

  val updateQuery: DynamoDBQuery[Widget, Option[Widget]] =
    update(widgets)(Widget.id.partitionKey === "w-1")(Widget.qty.set(5))

  val deleteQuery: DynamoDBQuery[Widget, Option[Widget]] =
    deleteFrom(widgets)(Widget.id.partitionKey === "w-1")

  val queryQuery: DynamoDBQuery[Widget, Page[Either[DynamoDBError.ItemError, Widget]]] =
    query(widgets, limit = 20).whereKey(Widget.id.partitionKey === "w-1").filter(nGte)

  val scanQuery: DynamoDBQuery[Widget, Page[Either[DynamoDBError.ItemError, Widget]]] =
    scan(widgets, limit = 20).filter(sBetween && nLte)
}
