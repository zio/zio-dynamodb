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
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

/**
 * A typed handle for a DynamoDB table: its name and the codec for `From`. Pass it to the CRUD
 * operations in [[DdbExprApiSyntax]] (`DdbExprApi` / `dsl`) in place of a bare table name - this is
 * what lets `From` be inferred at the call site (`query(orders, 20)` rather than
 * `query[Order]("orders", 20)`).
 *
 * `Table[Order]("orders")` derives the codec from `Order`'s `Schema` with the default
 * `DynamoDBCodecDeriver`. To configure it, derive the codec with ZIO Blocks' derivation builder and
 * pass it in - deriver-wide settings on the deriver, per-field settings by optic:
 *
 * {{{
 *   val orders = Table("orders",
 *     Schema[Order].deriving(DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.SnakeCase))
 *       .modifier(Order.customerId, Modifier.rename("cust"))
 *       .derive
 *   )
 * }}}
 *
 * Expressions on the table (`.where`, `.filter`, key conditions, updates) are resolved against this
 * codec, so their attribute paths and literal encodings always match the stored item. That needs a
 * codec derived by `DynamoDBCodecDeriver`: with a hand-written top-level codec, `put` and unfiltered
 * scans work, but anything with a path - key conditions included - fails in the effect. [[decode]] / [[encode]] expose the codec for the Low-Level `Item`-shaped operations the
 * High-Level API doesn't wrap (`batchGetItem`, `transactGetItems`, ...).
 *
 * Construct a `Table` once and reuse it: `Table[A](name)` derives its codec on first use and holds it,
 * so a `Table` rebuilt per request re-derives it each time.
 */
final class Table[From] private (val name: String, mkCodec: () => DynamoDBCodec[From]) {

  private[ddbexpr] lazy val entry: CodecEntry[From] = {
    val codec       = mkCodec()
    val projections = codec.recordFieldNames
      .map(field => ProjectionExpression.MapElement(ProjectionExpression.Root, field): ProjectionExpression[_, _])
    CodecEntry(codec, projections)
  }

  // Resolves `.where` / `.filter` / key-condition / update optics against the same codec `put` writes
  // with, so expressions can't disagree with the stored item.
  private[ddbexpr] lazy val exprCtx: ExprCtx = new ExprCtx(entry.codec)

  // Decodes the item a `.returning` write gets back; held here so building a write allocates no closure.
  private[ddbexpr] lazy val decodeReturned
    : Option[Item] => Option[Either[DynamoDBError.ItemError.DecodingError, From]] =
    BuilderSupport.decodeReturned(this)

  /**
   * Decodes an [[Item]] — as returned by the Low-Level API (`DynamoDBQuery.getItem`,
   * `batchGetItem`, `transactGetItems`, …) — into `From` with this table's codec.
   */
  def decode(item: Item): Either[DynamoDBError.ItemError, From] =
    entry.codec.fromItem(item)

  /**
   * Encodes `a` into an [[Item]] for the Low-Level API's `Item`-shaped operations, with this table's
   * codec. `Left` when `From`'s codec does not produce a top-level map.
   */
  def encode(a: From): Either[DynamoDBError, Item] =
    FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue(entry.codec.encoder(a))

  override def toString: String = s"Table($name)"
}

object Table {

  /** A table whose codec derives from `From`'s `Schema` with the default `DynamoDBCodecDeriver`, on first use. */
  def apply[From](name: String)(implicit schema: Schema[From]): Table[From] =
    new Table(name, () => schema.deriving(DynamoDBCodecDeriver).derive)

  /** A table with the given codec - typically derived with ZIO Blocks' derivation builder (see [[Table]]). */
  def apply[From](name: String, codec: DynamoDBCodec[From]): Table[From] =
    new Table(name, () => codec)
}
