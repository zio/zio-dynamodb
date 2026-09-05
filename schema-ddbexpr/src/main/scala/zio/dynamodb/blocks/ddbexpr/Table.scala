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
import zio.dynamodb.{ DynamoDBError, FromAttributeValue, Item, ProjectionExpression }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.ProjectionResolver

/**
 * A typed handle for a DynamoDB table: its name, the [[Schema]] for `From`, and the
 * [[DynamoDBCodecDeriverConfigure]] used to derive `From`'s codec. Construct one
 * (`Table[Order]("orders")`) and pass it to the CRUD operations in [[DdbExprApiSyntax]]
 * (`DdbExprApi` / `dsl`) in place of a bare `tableName: String` — this is what lets `From`
 * be inferred at the call site (`query(orders, 20)` rather than `query[Order]("orders", 20)`).
 *
 * `Schema[From]` is the only implicit `apply` needs — it comes from `derives Schema` on the
 * model. Codec-derivation config is attached to the value, not resolved from implicit scope:
 *
 * {{{
 *   val orders = Table[Order]("orders").deriving(
 *     _.withEnumValuesAsStrings(false).withFieldNameMapper(NameMapper.SnakeCase)
 *   )
 * }}}
 *
 * [[decode]] / [[encode]] expose the same configured codec for the Low-Level `Item`-shaped
 * operations the High-Level API does not wrap — `batchGetItem`, `transactGetItems`,
 * `transactWriteItems`, hand-rolled streaming, and so on — so that path stays consistent
 * with what `get` / `put` on this table do.
 *
 * Construct a `Table` once and reuse it: the derived codec and the projection list for its
 * fields are computed lazily on first use and held on the instance, so a `Table` rebuilt per
 * request re-derives its codec each time.
 */
final class Table[From] private (
  val name: String,
  private[ddbexpr] val schema: Schema[From],
  private[ddbexpr] val config: DynamoDBCodecDeriverConfigure[From]
) {

  private[ddbexpr] lazy val entry: CodecEntry[From] = {
    val codec       = schema.deriving(config.toDeriver).derive
    val projections = codec.recordFieldNames
      .map(field => ProjectionExpression.MapElement(ProjectionExpression.Root, field): ProjectionExpression[_, _])
    CodecEntry(codec, projections)
  }

  // Per-table expression-resolution context: threads this table's config and a
  // deriver-produced ProjectionResolver into `.where` / `.filter` / key-condition
  // interpretation, and memoises literal codecs, so construction allocates no cache keys.
  private[ddbexpr] lazy val exprCtx: ExprCtx =
    new ExprCtx(config, new ProjectionResolver(schema.deriving(config.toResolverDeriver).derive))

  /**
   * Returns a copy of this table whose codec derives with `configure` applied to the
   * default [[DynamoDBCodecDeriverConfigure]]. Replaces any configuration previously
   * attached. The config is a value with readable fields (`fieldNameMapper`,
   * `discriminatorKind`, per-field `rename`, …), not an opaque `Deriver` transform.
   */
  def deriving(
    configure: DynamoDBCodecDeriverConfigure[From] => DynamoDBCodecDeriverConfigure[From]
  ): Table[From] =
    new Table(name, schema, configure(config))

  /**
   * Decodes an [[Item]] — as returned by the Low-Level API (`DynamoDBQuery.getItem`,
   * `batchGetItem`, `transactGetItems`, …) — into `From` using this table's configured,
   * cached codec.
   */
  def decode(item: Item): Either[DynamoDBError.ItemError, From] =
    entry.codec.fromItem(item)

  /**
   * Encodes `a` into an [[Item]] for the Low-Level API's `Item`-shaped operations, using
   * this table's configured, cached codec. `Left` when `From`'s codec does not produce a
   * top-level map (a `Table[From]` whose `From` is not record-shaped).
   */
  def encode(a: From): Either[DynamoDBError, Item] =
    FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue(entry.codec.encoder(a))

  override def toString: String = s"Table($name)"
}

object Table {

  def apply[From](name: String)(implicit schema: Schema[From]): Table[From] =
    new Table(name, schema, DynamoDBCodecDeriverConfigure[From]())

  /** For when there is no `Schema[From]` in implicit scope. */
  def of[From](name: String, schema: Schema[From]): Table[From] =
    new Table(name, schema, DynamoDBCodecDeriverConfigure[From]())
}
