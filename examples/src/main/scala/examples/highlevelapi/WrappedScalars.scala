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

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBError, DynamoDBQuery, Page }
import zio.dynamodb.blocks.ddbexpr.DdbExpr
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.prelude.Subtype

/**
 * Wrapping a scalar field in your own type — an `AnyVal` value class or a zio-prelude
 * `Subtype` here (see `WrappedScalarsOpaqueType.scala` for a Scala 3 opaque type) — still
 * works with the query DSL, as long as the wrapper has its own `Schema` and `DynamoDBCodec`.
 */
object WrappedScalars {

  // AnyVal value class over S
  final case class Sku(value: String) extends AnyVal
  object Sku {
    implicit val schema: Schema[Sku]       = Schema.string.transform(Sku(_), _.value)
    implicit val codec: DynamoDBCodec[Sku] = schema.deriving(DynamoDBCodecDeriver).derive
  }

  // zio-prelude Subtype over N
  object Weight extends Subtype[Int] {
    implicit val schema: Schema[Weight.Type]       = Schema.int.transform(Weight.wrap, Weight.unwrap)
    implicit val codec: DynamoDBCodec[Weight.Type] = schema.deriving(DynamoDBCodecDeriver).derive
  }

  final case class Widget(sku: Sku, weight: Weight.Type)

  object Widget extends CompanionOptics[Widget] {
    implicit val schema: Schema[Widget] = Schema.derived

    val sku: Lens[Widget, Sku]            = $(_.sku)
    val weight: Lens[Widget, Weight.Type] = $(_.weight)
  }

  val widgets: Table[Widget] = Table[Widget]("widgets")

  // `===`/`>` work on a value class, but `between`/`in`/`inSet` do not — see
  // CompileTimeRejections.scala.
  val skuEq: DdbExpr[Widget, Boolean] = Widget.sku === Sku("SKU-1")

  // Wrapped[N] — zio-prelude Subtype
  val weightGt: DdbExpr[Widget, Boolean]      = Widget.weight > Weight(0)
  val weightBetween: DdbExpr[Widget, Boolean] = Widget.weight.between(Weight(0), Weight(100))
  val weightIn: DdbExpr[Widget, Boolean]      = Widget.weight.in(Weight(1), Weight(2), Weight(3))

  // Wired into the six CRUD operations — see WrappedScalarsSpec for these actually run.
  val putQuery: DynamoDBQuery[Widget, Option[Widget]] =
    put(widgets, Widget(Sku("SKU-1"), Weight(10)))

  val getQuery: DynamoDBQuery[Widget, Either[DynamoDBError.ItemError, Widget]] =
    get(widgets)(Widget.sku.partitionKey === Sku("SKU-1"))

  val updateQuery: DynamoDBQuery[Widget, Option[Widget]] =
    update(widgets)(Widget.sku.partitionKey === Sku("SKU-1"))(Widget.weight.set(Weight(20)))

  val deleteQuery: DynamoDBQuery[Widget, Option[Widget]] =
    deleteFrom(widgets)(Widget.sku.partitionKey === Sku("SKU-1"))

  val queryQuery: DynamoDBQuery[Widget, Page[Either[DynamoDBError.ItemError, Widget]]] =
    query(widgets, limit = 20).whereKey(Widget.sku.partitionKey === Sku("SKU-1")).filter(weightGt)

  val scanQuery: DynamoDBQuery[Widget, Page[Either[DynamoDBError.ItemError, Widget]]] =
    scan(widgets, limit = 20).filter(skuEq)
}
