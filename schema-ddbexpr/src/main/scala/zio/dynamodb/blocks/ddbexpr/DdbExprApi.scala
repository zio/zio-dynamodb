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

import scala.annotation.implicitNotFound
import scala.language.implicitConversions

private[ddbexpr] final case class CodecEntry[A](codec: DynamoDBCodec[A], projections: Chunk[ProjectionExpression[_, _]])

// Keyed by Schema (reference identity - Schema instances are per-type singletons) and
// DynamoDBCodecDeriverConfig (value equality - it is now a case class). Used by ExprCtx's
// per-literal codec cache.
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
  def decode[From](table: Table[From], item: Item): Either[DynamoDBError.ItemError.DecodingError, From] =
    table.decode(item).left.map {
      case d: DynamoDBError.ItemError.DecodingError => d
      case other                                    => DynamoDBError.ItemError.DecodingError.failure(other.message)
    }

  def decodeReturned[From](
    table: Table[From]
  ): Option[Item] => Option[Either[DynamoDBError.ItemError.DecodingError, From]] =
    _.map(decode(table, _))

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

// Never given: the builders declare `returns` / `returnValuesOnConditionCheckFailure` requiring
// these so that a call resolves to the builder's own (uncallable) member instead of silently going
// through the implicit conversion to DynamoDBQuery, which would contradict the builder's type.
@implicitNotFound("use .returning(...) - .returns here would discard the returned item")
sealed trait UseReturningInstead

@implicitNotFound(
  "conditional writes already return the existing item on failure - remove this call " +
    "(or use .withoutItemOnConditionFailure after .where to turn it off)"
)
sealed trait ItemOnConditionFailureIsDefault

/**
 * Returned by [[DdbExprApiSyntax.put]] / [[DdbExprApiSyntax.update]] / [[DdbExprApiSyntax.deleteFrom]]:
 * a [[zio.dynamodb.WriteBuilder]] whose `.returning` decodes the returned item with the table's codec
 * (`Left` when it doesn't decode), and whose `.where` also accepts [[DdbExpr]] / `SchemaExpr`
 * conditions. Converts implicitly to the underlying [[DynamoDBQuery]] (e.g. for `transactWriteItems`).
 */
final class TableWriteBuilder[From, Out] private[ddbexpr] (
  private[ddbexpr] val table: Table[From],
  write0: DynamoDBQuery[From, Option[Item]],
  finish0: Option[Item] => Out
) extends WriteBuilder[From, Out, Option[Either[DynamoDBError.ItemError.DecodingError, From]]](
      write0,
      finish0
    ) {

  /** See [[WriteBuilder.where]]. */
  def where(expr: DdbExpr[From, Boolean]): ConditionalTableWriteBuilder[From, Out]        =
    where(BuilderSupport.condition(expr, table))
  def where(se: SchemaExpr[From, Boolean]): ConditionalTableWriteBuilder[From, Out]       = where(DdbExpr.Builtin(se))
  override def where(ce: ConditionExpression[_]): ConditionalTableWriteBuilder[From, Out] =
    new ConditionalTableWriteBuilder(table, WriteBuilder.conditional(write, ce), finish)

  override def returning(
    rv: ReturnValues
  ): TableWriteBuilder[From, Option[Either[DynamoDBError.ItemError.DecodingError, From]]] =
    new TableWriteBuilder(table, write.returns(rv), table.decodeReturned)

  override def capacity(capacity: ReturnConsumedCapacity): TableWriteBuilder[From, Out]        =
    new TableWriteBuilder(table, write.capacity(capacity), finish)
  override def metrics(itemMetrics: ReturnItemCollectionMetrics): TableWriteBuilder[From, Out] =
    new TableWriteBuilder(table, write.metrics(itemMetrics), finish)
  override def withRetryPolicy(policy: RetryPolicy): TableWriteBuilder[From, Out]              =
    new TableWriteBuilder(table, write.withRetryPolicy(policy), finish)

  /** Does not compile: use [[returning]], which also changes the result type to the returned item. */
  def returns(rv: ReturnValues)(implicit useReturningInstead: UseReturningInstead): Nothing =
    sys.error(s"unreachable: $rv, $useReturningInstead")

  /** Does not compile: a conditional write (`.where`) returns the existing item on failure by default. */
  def returnValuesOnConditionCheckFailure(rv: ReturnValuesOnConditionCheckFailure)(implicit
    isDefault: ItemOnConditionFailureIsDefault
  ): Nothing =
    sys.error(s"unreachable: $rv, $isDefault")
}

/** A [[TableWriteBuilder]] with a condition - see [[ConditionalWriteBuilder]]. */
final class ConditionalTableWriteBuilder[From, Out] private[ddbexpr] (
  private[ddbexpr] val table: Table[From],
  write0: DynamoDBQuery[From, Option[Item]],
  finish0: Option[Item] => Out
) extends ConditionalWriteBuilder[From, Out, Option[Either[DynamoDBError.ItemError.DecodingError, From]]](
      write0,
      finish0
    ) {

  def where(expr: DdbExpr[From, Boolean]): ConditionalTableWriteBuilder[From, Out]        =
    where(BuilderSupport.condition(expr, table))
  def where(se: SchemaExpr[From, Boolean]): ConditionalTableWriteBuilder[From, Out]       = where(DdbExpr.Builtin(se))
  override def where(ce: ConditionExpression[_]): ConditionalTableWriteBuilder[From, Out] =
    new ConditionalTableWriteBuilder(table, write.withCondition(ce), finish)

  override def withoutItemOnConditionFailure: ConditionalTableWriteBuilder[From, Out] =
    new ConditionalTableWriteBuilder(table, WriteBuilder.withoutItemOnConditionFailure(write), finish)

  override def returning(
    rv: ReturnValues
  ): ConditionalTableWriteBuilder[From, Option[Either[DynamoDBError.ItemError.DecodingError, From]]] =
    new ConditionalTableWriteBuilder(table, write.returns(rv), table.decodeReturned)

  override def capacity(capacity: ReturnConsumedCapacity): ConditionalTableWriteBuilder[From, Out]        =
    new ConditionalTableWriteBuilder(table, write.capacity(capacity), finish)
  override def metrics(itemMetrics: ReturnItemCollectionMetrics): ConditionalTableWriteBuilder[From, Out] =
    new ConditionalTableWriteBuilder(table, write.metrics(itemMetrics), finish)
  override def withRetryPolicy(policy: RetryPolicy): ConditionalTableWriteBuilder[From, Out]              =
    new ConditionalTableWriteBuilder(table, write.withRetryPolicy(policy), finish)

  /** Does not compile: on by default here; use [[withoutItemOnConditionFailure]] to turn it off. */
  def returnValuesOnConditionCheckFailure(rv: ReturnValuesOnConditionCheckFailure)(implicit
    isDefault: ItemOnConditionFailureIsDefault
  ): Nothing =
    sys.error(s"unreachable: $rv, $isDefault")

  /** Does not compile: use [[returning]], which also changes the result type to the returned item. */
  def returns(rv: ReturnValues)(implicit useReturningInstead: UseReturningInstead): Nothing =
    sys.error(s"unreachable: $rv, $useReturningInstead")
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
 *  `deleteFrom` return a [[TableWriteBuilder]]. Chain `.whereKey` / `.filter` / `.where` on
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
 *  `.whereKey` / `.filter` / `.where` on the builders take `DdbKeyExpr` / `DdbExpr` /
 *  `SchemaExpr` through overloads on the builder, each interpreting the argument against the
 *  calling table's configuration. Interpretation failures surface at query execution via the
 *  `Failure` nodes in [[KeyConditionExpr]] and [[ConditionExpression]].
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

  def put[A](table: Table[A], a: A): TableWriteBuilder[A, Unit] =
    new TableWriteBuilder(
      table,
      table.encode(a) match {
        case Right(encodedItem) => DynamoDBQuery.putItem(table.name, encodedItem)
        case Left(err)          => DynamoDBQuery.fail(err)
      },
      WriteBuilder.discard
    )

  def get[From](
    table: Table[From]
  )(
    keyExpr: DdbKeyExpr.PrimaryKey[From]
  ): DynamoDBQuery[From, Option[Either[DynamoDBError.ItemError.DecodingError, From]]] = {
    val entry = table.entry
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr, table.exprCtx) match {
      case Right(pkExpr) =>
        DynamoDBQuery
          .getItem(table.name, pkExpr.asAttrMap, entry.projections: _*)
          .map(_.map(BuilderSupport.decode(table, _)))
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }
  }

  /**
   * Never retries automatically, unlike other operations — `action` may be a delta
   * (`.add`/`.increment`/`.decrement`/`.appendList`/`.prependList`), and retrying one after an
   * ambiguous-outcome failure can double-apply it. Attach a policy explicitly via
   * `.withRetryPolicy(...)` on the resulting query only when `action` is idempotent
   * (`.set`/`.remove`/`.deleteFromSet`).
   */
  def update[From](table: Table[From])(keyExpr: DdbKeyExpr.PrimaryKey[From])(
    action: DdbUpdateExpr[From]
  ): TableWriteBuilder[From, Unit] =
    updateWith(table, keyExpr)(DdbUpdateExprInterpreter.toAction(action, table.exprCtx))

  /**
   * Escape hatch for an already-built `core` [[UpdateExpression.Action]] — e.g. one assembled
   *  with the low-level `ProjectionExpression` syntax (`$("field").set(value)`), or composed
   *  by hand. Passed to `updateItem` verbatim: unlike a [[DdbUpdateExpr]], a raw `Action` was
   *  never derived from a `Schema` optic, so there is no table configuration for it to miss —
   *  the same role `where(ce: ConditionExpression[_])` plays on [[TableWriteBuilder]].
   *  Same retry caveat as [[update]] applies to `action` here.
   */
  // A separate name rather than a third `update` overload: two curried methods identical
  // except in the last parameter list resolve fine under Scala 3 but are ambiguous under
  // Scala 2.13's overload resolution for a call site holding the argument in a typed `val`.
  def updateAction[From](table: Table[From])(keyExpr: DdbKeyExpr.PrimaryKey[From])(
    action: UpdateExpression.Action[From]
  ): TableWriteBuilder[From, Unit] =
    updateWith(table, keyExpr)(action)

  private def updateWith[From](table: Table[From], keyExpr: DdbKeyExpr.PrimaryKey[From])(
    action: UpdateExpression.Action[From]
  ): TableWriteBuilder[From, Unit] =
    new TableWriteBuilder(
      table,
      DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr, table.exprCtx) match {
        case Right(pkExpr) =>
          DynamoDBQuery.updateItem(table.name, pkExpr.asAttrMap)(action)
        case Left(msg)     =>
          DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
      },
      WriteBuilder.discard
    )

  def deleteFrom[From](
    table: Table[From]
  )(keyExpr: DdbKeyExpr.PrimaryKey[From]): TableWriteBuilder[From, Unit] =
    new TableWriteBuilder(
      table,
      DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr, table.exprCtx) match {
        case Right(pkExpr) =>
          DynamoDBQuery.deleteItem(table.name, pkExpr.asAttrMap)
        case Left(msg)     =>
          DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
      },
      WriteBuilder.discard
    )

  /**
   * A condition-only transact-write action with no mutation of its own — guards the whole
   *  transaction on `table`'s state at `keyExpr` without writing anything itself. Interprets
   *  `condition` with `table`'s deriver configuration, exactly like `.where` on [[TableWriteBuilder]].
   *  A bare `SchemaExpr[From, Boolean]` (e.g. `Account.status === "open"`) works here too, via
   *  [[DdbExprSyntax.schemaExprToDdbExpr]] — no second overload needed (and a second overload,
   *  curried the same way, is ambiguous under Scala 2.13 for a call site holding the condition
   *  in a typed `val`). Returned directly as a [[DynamoDBQuery]] (not a builder) since there's
   *  nothing further to attach; `.returnValuesOnConditionCheckFailure` is already a general
   *  `DynamoDBQuery` method.
   */
  def conditionCheck[From](table: Table[From])(keyExpr: DdbKeyExpr.PrimaryKey[From])(
    condition: DdbExpr[From, Boolean]
  ): DynamoDBQuery[From, Unit] =
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr, table.exprCtx) match {
      case Right(pkExpr) =>
        DynamoDBQuery.conditionCheck(table.name, pkExpr.asAttrMap)(BuilderSupport.condition(condition, table))
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }

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

  implicit def writeBuilderToQuery[From, Out](b: TableWriteBuilder[From, Out]): DynamoDBQuery[From, Out] = b.toQuery

  implicit def conditionalWriteBuilderToQuery[From, Out](
    b: ConditionalTableWriteBuilder[From, Out]
  ): DynamoDBQuery[From, Out] =
    b.toQuery

}

/**
 * The [[DdbExprApiSyntax]] singleton - `import DdbExprApi._` for explicit-object-style
 * access; see the `dsl` facade for the single-import alternative.
 */
object DdbExprApi extends DdbExprApiSyntax
