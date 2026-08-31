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

import zio.blocks.chunk.Chunk
import zio.blocks.schema.SchemaExpr
import zio.dynamodb._
import zio.dynamodb.blocks.schema.DynamoDBCodec

private[ddbexpr] final case class CodecEntry[A](codec: DynamoDBCodec[A], projections: Chunk[ProjectionExpression[_, _]])

// Keyed by (Schema, DynamoDBCodecDeriverConfigure) reference identity — avoids
// cross-classloader collisions and ensures types with custom configures get their
// own entry. Used by DerivedCodecSyntax's expression-building codec cache.
private[ddbexpr] final class CodecCacheKey(private val r0: AnyRef, private val r1: AnyRef) {
  override val hashCode: Int           = System.identityHashCode(r0) * 31 + System.identityHashCode(r1)
  override def equals(o: Any): Boolean = o match {
    case k: CodecCacheKey => (r0 eq k.r0) && (r1 eq k.r1)
    case _                => false
  }
}

/**
 * High-level CRUD API backed by [[DdbExpr]] condition expressions and [[DdbKeyExpr]]
 *  key condition expressions.
 *
 *  Each operation takes a [[Table]] rather than a bare table name: build one
 *  `Table[A]("name")` per table — only `Schema[A]` is needed implicitly (from `derives
 *  Schema`); attach any deriver configuration with `Table`'s `.deriving(...)`. Its codec is
 *  derived once and held on the value. Passing the `Table` rather than a `String` is also
 *  what lets `A` be inferred for `query` / `scan`, which otherwise mention it nowhere in
 *  their arguments.
 *
 *  All ZB [[zio.blocks.schema.Optic]] operators (===, >, <, >=, <=) encode sealed-trait
 *  literals correctly. Since zio-blocks v0.0.47
 *  [[zio.blocks.schema.DynamicSchemaExpr.Literal]] carries a [[zio.blocks.schema.Schema]], the
 *  interpreter derives a [[zio.dynamodb.blocks.schema.DynamoDBCodec]] at evaluation time —
 *  `enumValuesAsStrings` and other encoding rules are preserved automatically.
 *
 *  Usage:
 *  {{{
 *    import DdbExprApi._
 *    import DdbKeyExpr._
 *    import DdbExpr.{ DdbExprBoolSyntax, OpticDdbExprOps, OpticStringDdbExprOps, SchemaExprBoolBridge }
 *
 *    val tasks = Table[Task]("tasks")
 *
 *    // CRUD
 *    DdbExprApi.put(tasks, task)
 *    DdbExprApi.get(tasks)(Task.id.partitionKey === "t1")
 *    DdbExprApi.deleteFrom(tasks)(Task.id.partitionKey === "t1")
 *
 *    // scalars and sealed traits — ZB Optic operators, lifted to Builtin
 *    DdbExprApi.scan(tasks, 20).filter(Task.score > 0)
 *    DdbExprApi.scan(tasks, 20).filter(Task.priority === Priority.High)
 *
 *    // DDB functions + combinators
 *    DdbExprApi.query(tasks, 20)
 *      .whereKey(Task.id.partitionKey === "alice" && Task.score.sortKey > 10)
 *      .filter(Task.name.beginsWith("A") && Task.score.between(1, 100))
 *  }}}
 *
 *  Importing [[DdbExprApi]]`._ ` brings the implicit conversions
 *  [[DdbExprApiSyntax.ddbKeyExprToKeyConditionExpr]], [[DdbExprApiSyntax.ddbExprToConditionExpression]], and
 *  [[DdbExprApiSyntax.schemaExprToConditionExpression]] into scope, enabling `.whereKey(DdbKeyExpr)`,
 *  `.filter(DdbExpr)`, and `.filter(SchemaExpr)` on any [[DynamoDBQuery]].
 *  Interpretation failures are deferred to query execution via the `Failure` nodes in
 *  [[KeyConditionExpr]] and [[ConditionExpression]].
 */
trait DdbExprApiSyntax {

  // Re-export of the package-level `Table` so `DdbExprApi.Table` / `dsl.Table` (and an
  // unqualified `Table` under `import DdbExprApi._` / `import dsl._`) all resolve to the
  // single canonical type — it stays package-level rather than nested here because this
  // trait is mixed into more than one object (DdbExprApi, dsl), and a nested class would
  // give each a distinct path-dependent `Table`.
  final type Table[From] = zio.dynamodb.blocks.ddbexpr.Table[From]
  final val Table: zio.dynamodb.blocks.ddbexpr.Table.type = zio.dynamodb.blocks.ddbexpr.Table

  // ── CRUD operations ───────────────────────────────────────────────────────────

  def put[A](table: Table[A], a: A): DynamoDBQuery[A, Option[A]] =
    table.encode(a) match {
      case Right(encodedItem) =>
        DynamoDBQuery
          .putItem(table.name, encodedItem)
          .map(_.flatMap(prevItem => table.decode(prevItem).toOption))
      case Left(err)          =>
        DynamoDBQuery.fail(err)
    }

  def get[From](
    table: Table[From]
  )(keyExpr: DdbKeyExpr.PrimaryKey[From]): DynamoDBQuery[From, Either[DynamoDBError.ItemError, From]] =
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr) match {
      case Right(pkExpr) =>
        val pkAttrMap = pkExpr.asAttrMap
        DynamoDBQuery.getItem(table.name, pkAttrMap, table.entry.projections: _*).map {
          case Some(item) => table.decode(item)
          case None       => Left(DynamoDBError.ItemError.ValueNotFound(s"value with key $pkAttrMap not found"))
        }
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }

  def update[From](table: Table[From])(keyExpr: DdbKeyExpr.PrimaryKey[From])(
    action: UpdateExpression.Action[From]
  ): DynamoDBQuery[From, Option[From]] =
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr) match {
      case Right(pkExpr) =>
        DynamoDBQuery
          .updateItem(table.name, pkExpr.asAttrMap)(action)
          .map(_.flatMap(item => table.decode(item).toOption))
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }

  def deleteFrom[From](
    table: Table[From]
  )(keyExpr: DdbKeyExpr.PrimaryKey[From]): DynamoDBQuery[From, Option[From]] =
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr) match {
      case Right(pkExpr) =>
        DynamoDBQuery
          .deleteItem(table.name, pkExpr.asAttrMap)
          .map(_.flatMap(item => table.decode(item).toOption))
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }

  // query and scan return a base query; callers chain .whereKey(DdbKeyExpr) and
  // .filter(DdbExpr) via the implicit conversions below.
  def query[From](table: Table[From], limit: Int): DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] =
    DynamoDBQuery
      .query(table.name, limit)
      .map(page =>
        Page(
          items = page.items.map(item => table.decode(item)),
          lastEvaluatedKey = page.lastEvaluatedKey,
          count = page.count,
          scannedCount = page.scannedCount
        )
      )

  def scan[From](table: Table[From], limit: Int): DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] =
    DynamoDBQuery
      .scan(table.name, limit)
      .map(page =>
        Page(
          items = page.items.map(item => table.decode(item)),
          lastEvaluatedKey = page.lastEvaluatedKey,
          count = page.count,
          scannedCount = page.scannedCount
        )
      )

  // ── Implicit conversions ──────────────────────────────────────────────────────

  // Enables .whereKey(ddbKeyExpr) on any DynamoDBQuery.
  // Interpretation failures are deferred to execution via KeyConditionExpr.Failure.
  implicit def ddbKeyExprToKeyConditionExpr[S](expr: DdbKeyExpr[S]): KeyConditionExpr[S] =
    DdbKeyExprInterpreter.toKeyConditionExpr(expr).fold(KeyConditionExpr.Failure(_), identity)

  // Enables .filter(ddbExpr) and .where(ddbExpr) on any DynamoDBQuery.
  // FilterExpression[-From] is a type alias for ConditionExpression[-From].
  // Interpretation failures are deferred to execution via ConditionExpression.Failure.
  implicit def ddbExprToConditionExpression[S](expr: DdbExpr[S, Boolean]): ConditionExpression[S] =
    DdbExprInterpreter.toConditionExpression(expr).fold(ConditionExpression.Failure(_), identity)

  // Enables .filter(Task.score > 0) or .filter(Task.priority === Priority.High) where
  // the argument is a ZB SchemaExpr. Goes through the Builtin path; the interpreter
  // derives a DynamoDBCodec from the embedded Schema[_] in each Literal node.
  implicit def schemaExprToConditionExpression[S](se: SchemaExpr[S, Boolean]): ConditionExpression[S] =
    ddbExprToConditionExpression(DdbExpr.Builtin(se))
}

/**
 * The [[DdbExprApiSyntax]] singleton — `import DdbExprApi._` for explicit-object-style
 * access; see the `dsl` facade for the single-import alternative.
 */
object DdbExprApi extends DdbExprApiSyntax
