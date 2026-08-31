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

package zio.dynamodb.blocks.ddbexpr

import zio.blocks.schema.Schema
import zio.blocks.schema.derive.Deriver
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

/**
 * A typed handle for a DynamoDB table: its name, the [[Schema]] for `From`, and the
 * configuration used to derive `From`'s [[DynamoDBCodec]]. Construct one
 * (`Table[Order]("orders")`) and pass it to the CRUD operations in [[DdbExprApiSyntax]]
 * (`DdbExprApi` / `dsl`) in place of a bare `tableName: String` — this is what lets `From`
 * be inferred at the call site (`query(orders, 20)` rather than `query[Order]("orders", 20)`).
 *
 * `Schema[From]` is the only implicit `apply` needs — it comes from `derives Schema` on the
 * model. Deriver configuration is attached to the value, not resolved from implicit scope:
 *
 * {{{
 *   val orders = Table[Order]("orders").deriving(
 *     _.withEnumValuesAsStrings(false).withFieldNameMapper(NameMapper.SnakeCase)
 *   )
 * }}}
 *
 * Construct a `Table` once and reuse it: the [[DynamoDBCodec]] and the projection list for
 * its fields are derived lazily on first use and held on the instance, so a `Table` rebuilt
 * per request re-derives its codec each time.
 */
final class Table[From] private (
  val name: String,
  private[ddbexpr] val schema: Schema[From],
  private val configureDeriver: DynamoDBCodecDeriver => Deriver[DynamoDBCodec]
) {

  private[ddbexpr] lazy val entry: CodecEntry[From] = {
    val codec       = schema.deriving(configureDeriver(DynamoDBCodecDeriver)).derive
    val projections = codec.recordFieldNames
      .map(field => ProjectionExpression.MapElement(ProjectionExpression.Root, field): ProjectionExpression[_, _])
    CodecEntry(codec, projections)
  }

  /**
   * Returns a copy of this table whose row [[DynamoDBCodec]] is derived with `configure`
   * applied to the base [[DynamoDBCodecDeriver]]. Replaces any configuration previously
   * attached. Put deriver-wide flags (`withEnumValuesAsStrings`, `withFieldNameMapper`,
   * `withSchema1TupleCompatibility`, …) before any per-field `withModifier` / `withInstance`
   * calls within `configure`.
   */
  def deriving(configure: DynamoDBCodecDeriver => Deriver[DynamoDBCodec]): Table[From] =
    new Table(name, schema, configure)

  override def toString: String = s"Table($name)"
}

object Table {

  def apply[From](name: String)(implicit schema: Schema[From]): Table[From] =
    new Table(name, schema, (d: DynamoDBCodecDeriver) => d)

  /** For when there is no `Schema[From]` in implicit scope. */
  def of[From](name: String, schema: Schema[From]): Table[From] =
    new Table(name, schema, (d: DynamoDBCodecDeriver) => d)
}
