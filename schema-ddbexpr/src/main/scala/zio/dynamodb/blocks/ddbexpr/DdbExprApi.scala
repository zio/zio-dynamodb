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

import scala.language.implicitConversions

private[ddbexpr] final case class CodecEntry[A](codec: DynamoDBCodec[A], projections: Chunk[ProjectionExpression[_, _]])

// Keyed by Schema (reference identity - Schema instances are per-type singletons) and
// DynamoDBCodecDeriverConfigure (value equality - it is now a case class). Used by
// DerivedCodecSyntax's expression-building codec cache.
private[ddbexpr] final class CodecCacheKey(private val r0: AnyRef, private val r1: AnyRef) {
  override val hashCode: Int           = System.identityHashCode(r0) * 31 + r1.hashCode
  override def equals(o: Any): Boolean = o match {
    case k: CodecCacheKey => (r0 eq k.r0) && (r1 == k.r1)
    case _                => false
  }
}

// Shared helper for the builders below: interpret a DdbExpr / SchemaExpr condition with the
// originating table's deriver configuration, so `.filter` / `.where` resolve the same
// attribute names and encode literals the same way as the item body. Kept top-level (not
// on DdbExprApiSyntax, which is mixed into more than one object) so there is a single
// canonical helper regardless of which facade the call site imported.
private[ddbexpr] object BuilderSupport {
  def condition[From](expr: DdbExpr[From, Boolean], table: Table[From]): ConditionExpression[From] =
    DdbExprInterpreter
      .toConditionExpression(expr, table.exprCtx)
      .fold(ConditionExpression.Failure(_), identity)
}

/**
 * Chainable builder returned by [[DdbExprApiSyntax.scan]]. `.filter` interprets its
 *  argument with the originating [[Table]]'s deriver configuration so filtered attribute
 *  names and encoded literals match what `put` writes for the item body. Converts
 *  implicitly to the underlying [[DynamoDBQuery]] (so `interpreter.run(scan(t, 20))` and
 *  `.map` chains keep working); `.execute` is also provided directly.
 */
final class ScanBuilder[From] private[ddbexpr] (
  private[ddbexpr] val table: Table[From],
  private[ddbexpr] val query: DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]]
) {
  private def attach(ce: ConditionExpression[From]): ScanBuilder[From] =
    new ScanBuilder(table, query.filter(ce))

  def filter(expr: DdbExpr[From, Boolean]): ScanBuilder[From]  = attach(BuilderSupport.condition(expr, table))
  def filter(se: SchemaExpr[From, Boolean]): ScanBuilder[From] = filter(DdbExpr.Builtin(se))
  def filter(ce: ConditionExpression[From]): ScanBuilder[From] = attach(ce)

  def toQuery: DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] = query

  def execute[F[_]](implicit interpreter: Interpreter[F]): F[Page[Either[DynamoDBError.ItemError, From]]] =
    interpreter.run(query)
}

/**
 * Chainable builder returned by [[DdbExprApiSyntax.query]]. `.whereKey` and `.filter`
 *  interpret with the originating [[Table]]'s deriver configuration. Converts implicitly
 *  to the underlying [[DynamoDBQuery]]; `.execute` is also provided directly.
 */
final class QueryBuilder[From] private[ddbexpr] (
  private[ddbexpr] val table: Table[From],
  private[ddbexpr] val query: DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]]
) {
  private def attach(ce: ConditionExpression[From]): QueryBuilder[From] =
    new QueryBuilder(table, query.filter(ce))

  def whereKey(expr: DdbKeyExpr[From]): QueryBuilder[From] = {
    val kce = DdbKeyExprInterpreter
      .toKeyConditionExpr(expr, table.exprCtx)
      .fold(KeyConditionExpr.Failure(_), identity)
    new QueryBuilder(table, query.whereKey(kce))
  }

  def filter(expr: DdbExpr[From, Boolean]): QueryBuilder[From]  = attach(BuilderSupport.condition(expr, table))
  def filter(se: SchemaExpr[From, Boolean]): QueryBuilder[From] = filter(DdbExpr.Builtin(se))
  def filter(ce: ConditionExpression[From]): QueryBuilder[From] = attach(ce)

  def toQuery: DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] = query

  def execute[F[_]](implicit interpreter: Interpreter[F]): F[Page[Either[DynamoDBError.ItemError, From]]] =
    interpreter.run(query)
}

/**
 * Chainable builder returned by [[DdbExprApiSyntax.put]] / [[DdbExprApiSyntax.update]] /
 *  [[DdbExprApiSyntax.deleteFrom]]. `.where` interprets its argument with the originating
 *  [[Table]]'s deriver configuration. Converts implicitly to the underlying
 *  [[DynamoDBQuery]]; `.execute` is also provided directly.
 */
final class WriteBuilder[From] private[ddbexpr] (
  private[ddbexpr] val table: Table[From],
  private[ddbexpr] val query: DynamoDBQuery[From, Option[From]]
) {
  private def attach(ce: ConditionExpression[From]): WriteBuilder[From] =
    new WriteBuilder(table, query.where(ce))

  def where(expr: DdbExpr[From, Boolean]): WriteBuilder[From]  = attach(BuilderSupport.condition(expr, table))
  def where(se: SchemaExpr[From, Boolean]): WriteBuilder[From] = where(DdbExpr.Builtin(se))
  def where(ce: ConditionExpression[From]): WriteBuilder[From] = attach(ce)

  def toQuery: DynamoDBQuery[From, Option[From]] = query

  def execute[F[_]](implicit interpreter: Interpreter[F]): F[Option[From]] =
    interpreter.run(query)
}

/**
 * High-level CRUD API backed by [[DdbExpr]] condition expressions and [[DdbKeyExpr]]
 *  key condition expressions.
 *
 *  Each operation takes a [[Table]] rather than a bare table name: build one
 *  `Table[A]("name")` per table - only `Schema[A]` is needed implicitly (from `derives
 *  Schema`); attach any deriver configuration with `Table`'s `.deriving(...)`. Its codec is
 *  derived once and held on the value. Passing the `Table` rather than a `String` is also
 *  what lets `A` be inferred for `query` / `scan`, which otherwise mention it nowhere in
 *  their arguments.
 *
 *  `query` / `scan` return a [[QueryBuilder]] / [[ScanBuilder]]; `put` / `update` /
 *  `deleteFrom` return a [[WriteBuilder]]. Chain `.whereKey` / `.filter` / `.where` on
 *  them - each is interpreted with the table's deriver configuration, so filtered and
 *  conditioned attribute names and literal encodings match what `put` writes for the item
 *  body. The builders convert implicitly to the underlying [[DynamoDBQuery]] and also
 *  expose `.execute` directly.
 *
 *  All ZB [[zio.blocks.schema.Optic]] operators (===, >, <, >=, <=) encode sealed-trait
 *  literals correctly. Since zio-blocks v0.0.47
 *  [[zio.blocks.schema.DynamicSchemaExpr.Literal]] carries a [[zio.blocks.schema.Schema]], the
 *  interpreter derives a [[zio.dynamodb.blocks.schema.DynamoDBCodec]] at evaluation time -
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
 *    // scalars and sealed traits - ZB Optic operators, lifted to Builtin
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
 *  `.filter(DdbExpr)`, and `.filter(SchemaExpr)` on any low-level [[DynamoDBQuery]] (these
 *  do not carry table configuration - the builders returned by `query` / `scan` do).
 *  Interpretation failures are deferred to query execution via the `Failure` nodes in
 *  [[KeyConditionExpr]] and [[ConditionExpression]].
 */
trait DdbExprApiSyntax {

  // Re-export of the package-level `Table` so `DdbExprApi.Table` / `dsl.Table` (and an
  // unqualified `Table` under `import DdbExprApi._` / `import dsl._`) all resolve to the
  // single canonical type - it stays package-level rather than nested here because this
  // trait is mixed into more than one object (DdbExprApi, dsl), and a nested class would
  // give each a distinct path-dependent `Table`.
  final type Table[From] = zio.dynamodb.blocks.ddbexpr.Table[From]
  final val Table: zio.dynamodb.blocks.ddbexpr.Table.type = zio.dynamodb.blocks.ddbexpr.Table

  // -- CRUD operations ----------------------------------------------------------

  def put[A](table: Table[A], a: A): WriteBuilder[A] =
    new WriteBuilder(
      table,
      table.encode(a) match {
        case Right(encodedItem) =>
          DynamoDBQuery
            .putItem(table.name, encodedItem)
            .map(_.flatMap(prevItem => table.decode(prevItem).toOption))
        case Left(err)          =>
          DynamoDBQuery.fail(err)
      }
    )

  def get[From](
    table: Table[From]
  )(keyExpr: DdbKeyExpr.PrimaryKey[From]): DynamoDBQuery[From, Either[DynamoDBError.ItemError, From]] = {
    val entry = table.entry
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr, table.exprCtx) match {
      case Right(pkExpr) =>
        val pkAttrMap = pkExpr.asAttrMap
        DynamoDBQuery.getItem(table.name, pkAttrMap, entry.projections: _*).map {
          case Some(item) => table.decode(item)
          case None       => Left(DynamoDBError.ItemError.ValueNotFound(s"value with key $pkAttrMap not found"))
        }
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }
  }

  def update[From](table: Table[From])(keyExpr: DdbKeyExpr.PrimaryKey[From])(
    action: UpdateExpression.Action[From]
  ): WriteBuilder[From] =
    new WriteBuilder(
      table,
      DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr, table.exprCtx) match {
        case Right(pkExpr) =>
          DynamoDBQuery
            .updateItem(table.name, pkExpr.asAttrMap)(action)
            .map(_.flatMap(item => table.decode(item).toOption))
        case Left(msg)     =>
          DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
      }
    )

  def deleteFrom[From](
    table: Table[From]
  )(keyExpr: DdbKeyExpr.PrimaryKey[From]): WriteBuilder[From] =
    new WriteBuilder(
      table,
      DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr, table.exprCtx) match {
        case Right(pkExpr) =>
          DynamoDBQuery
            .deleteItem(table.name, pkExpr.asAttrMap)
            .map(_.flatMap(item => table.decode(item).toOption))
        case Left(msg)     =>
          DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
      }
    )

  def query[From](table: Table[From], limit: Int): QueryBuilder[From] =
    new QueryBuilder(table, mapPage(DynamoDBQuery.query(table.name, limit), table))

  def scan[From](table: Table[From], limit: Int): ScanBuilder[From] =
    new ScanBuilder(table, mapPage(DynamoDBQuery.scan(table.name, limit), table))

  private def mapPage[From](
    q: DynamoDBQuery[Any, Page[Item]],
    table: Table[From]
  ): DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] =
    q.map(page =>
      Page(
        items = page.items.map(item => table.decode(item)),
        lastEvaluatedKey = page.lastEvaluatedKey,
        count = page.count,
        scannedCount = page.scannedCount
      )
    ).asInstanceOf[DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]]]

  // -- Builder -> DynamoDBQuery implicit conversions ---------------------------

  implicit def scanBuilderToQuery[From](
    b: ScanBuilder[From]
  ): DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] = b.toQuery

  implicit def queryBuilderToQuery[From](
    b: QueryBuilder[From]
  ): DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] = b.toQuery

  implicit def writeBuilderToQuery[From](b: WriteBuilder[From]): DynamoDBQuery[From, Option[From]] = b.toQuery

  // -- Implicit conversions (low-level path - no table configuration) ----------

  // Enables .whereKey(ddbKeyExpr) on any low-level DynamoDBQuery.
  // Interpretation failures are deferred to execution via KeyConditionExpr.Failure.
  implicit def ddbKeyExprToKeyConditionExpr[S](expr: DdbKeyExpr[S]): KeyConditionExpr[S] =
    DdbKeyExprInterpreter.toKeyConditionExpr(expr).fold(KeyConditionExpr.Failure(_), identity)

  // Enables .filter(ddbExpr) and .where(ddbExpr) on any low-level DynamoDBQuery.
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
 * The [[DdbExprApiSyntax]] singleton - `import DdbExprApi._` for explicit-object-style
 * access; see the `dsl` facade for the single-import alternative.
 */
object DdbExprApi extends DdbExprApiSyntax
