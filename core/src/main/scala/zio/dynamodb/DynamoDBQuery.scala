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

package zio.dynamodb

import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.{ Delete, Put }
import zio.dynamodb.DynamoDBQuery.ZipPar
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.Utils.ListUtils
import zio.dynamodb.proofs.{ CanFilter, CanWhere }

import scala.+:

// `map`/`flatMap` exist so a query-composition test can sequence several `.execute` calls in
// a for-comprehension, same shape as it would under a real effect type - not a general-purpose
// effect type of its own.
final case class DummyIO[+A](unsafeRun: () => A) {
  def map[B](f: A => B): DummyIO[B]              = DummyIO(() => f(unsafeRun()))
  def flatMap[B](f: A => DummyIO[B]): DummyIO[B] = DummyIO(() => f(unsafeRun()).unsafeRun())
}
object DummyIO                                   {
  def succeed[A](a: => A): DummyIO[A] = DummyIO(() => a)
}

/**
 * A description of a DynamoDB request (or a composition of several) that stays independent
 * of any effect type until it's run. `In` (contravariant) is a phantom type used to prove, at
 * compile time via [[proofs.CanWhere]]/[[proofs.CanFilter]], that a `.where`/`.filter`
 * condition attaches to a query it's actually type-compatible with; `Out` (covariant) is the
 * value the query produces once run.
 *
 * Sealed subtypes are either leaf `Constructor`s (`GetItem`, `PutItem`, `Query`, ...,
 * each shaped directly after one AWS SDK operation) or combinators (`Map`, `ZipPar`, ...)
 * built by chaining methods on an existing query. Nothing about a `DynamoDBQuery` value runs
 * anything — an interpreter (`ZioInterpreter`/`CEInterpreter`/`FutureInterpreter`, all
 * extending [[AwsInterpreter]]) walks the ADT and issues the real AWS SDK calls.
 */
sealed trait DynamoDBQuery[-In, +Out] { self =>

  final def map[B](f: Out => B): DynamoDBQuery[In, B] = DynamoDBQuery.Map(self, f)

  /** Runs `self` and `that` in parallel and combines their results. */
  final def zipPar[In1 <: In, B](that: DynamoDBQuery[In1, B])(implicit
    z: Zippable[Out, B]
  ): DynamoDBQuery[In1, z.Out] =
    DynamoDBQuery.ZipPar[Out, B, z.Out](self, that, z)

  final def zipParLeft[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, Out] =
    (self zipPar that).map(_._1)

  final def zipParRight[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, B] =
    (self zipPar that).map(_._2)

  final def zipParWith[In1 <: In, B, C](that: DynamoDBQuery[In1, B])(
    f: (Out, B) => C
  ): DynamoDBQuery[In1, C] =
    (self zipPar that).map(f.tupled)

  // Same default as a write builder's `.where`: a failed condition returns the existing item.
  def where[B](conditionExpression: ConditionExpression[B])(implicit ev: CanWhere[B, Out]): DynamoDBQuery[In, Out] = {
    val _ = ev
    withConditionReturningItemOnFailure(conditionExpression)
  }

  // A write builder's `.where`: the condition plus the item-on-failure default, in a single copy.
  private[dynamodb] def withConditionReturningItemOnFailure(
    conditionExpression: ConditionExpression[_]
  ): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(
          left.withConditionReturningItemOnFailure(conditionExpression),
          right.withConditionReturningItemOnFailure(conditionExpression),
          zippable
        )
      case b: BuilderNode[_, _]             =>
        b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].withConditionReturningItemOnFailure(conditionExpression)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.withConditionReturningItemOnFailure(conditionExpression), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.withConditionReturningItemOnFailure(conditionExpression))
      case p: DynamoDBQuery.PutItem         =>
        p.copy(
          conditionExpression = Some(conditionExpression),
          returnValuesOnConditionCheckFailure = DynamoDBQuery.itemOnFailure
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem      =>
        u.copy(
          conditionExpression = Some(conditionExpression),
          returnValuesOnConditionCheckFailure = DynamoDBQuery.itemOnFailure
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem      =>
        d.copy(
          conditionExpression = Some(conditionExpression),
          returnValuesOnConditionCheckFailure = DynamoDBQuery.itemOnFailure
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => withCondition(conditionExpression)
    }

  // `where` without the CanWhere check, for builders that already know they hold a write.
  private[dynamodb] def withCondition(conditionExpression: ConditionExpression[_]): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(left.withCondition(conditionExpression), right.withCondition(conditionExpression), zippable)
      case b: BuilderNode[_, _]             => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].withCondition(conditionExpression)
      case DynamoDBQuery.Map(query, mapper) => DynamoDBQuery.Map(query.withCondition(conditionExpression), mapper)
      case DynamoDBQuery.Absolve(query)     => DynamoDBQuery.Absolve(query.withCondition(conditionExpression))
      case p: DynamoDBQuery.PutItem         =>
        p.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem      =>
        u.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem      =>
        d.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                =>
        failUnsupported(
          s"'.where' has no effect on ${self.getClass.getSimpleName}; condition expressions are only " +
            s"supported on putItem/updateItem/deleteItem — did you mean '.filter'?"
        )
    }

  /**
   * Adds a KeyConditionExpr to a DynamoDBQuery. Example:
   * {{{
   * // high level type safe API where "email" and "subject" keys are defined using ProjectionExpression.accessors[Student]
   * val newQuery = query.whereKey(email.partitionKey === "avi@gmail.com" && subject.sortKey === "maths")
   *
   * // low level API
   * val newQuery = query.whereKey($("email").partitionKey === "avi@gmail.com" && $("subject").sortKey === "maths")
   * }}}
   */
  def whereKey[From](keyConditionExpression: KeyConditionExpr[From]): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(left.whereKey(keyConditionExpression), right.whereKey(keyConditionExpression), zippable)
      case b: BuilderNode[_, _]                        => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].whereKey(keyConditionExpression)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.whereKey(keyConditionExpression), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.whereKey(keyConditionExpression))

      case s: DynamoDBQuery.Query =>
        s.copy(keyConditionExpr = Some(keyConditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                      =>
        failUnsupported(
          s"'.whereKey' has no effect on ${self.getClass.getSimpleName}; key condition expressions are " +
            s"only supported on query"
        )
    }

  private[dynamodb] final def returnValuesOnConditionCheckFailure(
    rv: ReturnValuesOnConditionCheckFailure
  ): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(
          left.returnValuesOnConditionCheckFailure(rv),
          right.returnValuesOnConditionCheckFailure(rv),
          zippable
        )
      case b: BuilderNode[_, _]             =>
        b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].returnValuesOnConditionCheckFailure(rv)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.returnValuesOnConditionCheckFailure(rv), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.returnValuesOnConditionCheckFailure(rv))
      case p: DynamoDBQuery.PutItem         =>
        p.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem      =>
        u.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem      =>
        d.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case c: DynamoDBQuery.ConditionCheck  =>
        c.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                =>
        unsupportedModifier("returnValuesOnConditionCheckFailure", "putItem/updateItem/deleteItem/conditionCheck")
    }

  final def withClientRequestToken(token: String): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)        =>
        ZipPar(left.withClientRequestToken(token), right.withClientRequestToken(token), zippable)
      case b: BuilderNode[_, _]                 => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].withClientRequestToken(token)
      case DynamoDBQuery.Map(query, mapper)     => DynamoDBQuery.Map(query.withClientRequestToken(token), mapper)
      case DynamoDBQuery.Absolve(query)         => DynamoDBQuery.Absolve(query.withClientRequestToken(token))
      case tw: DynamoDBQuery.TransactWriteItems =>
        tw.copy(clientRequestToken = Some(token)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                    => unsupportedModifier("withClientRequestToken", "transactWriteItems")
    }

  def withRetryPolicy(policy: RetryPolicy): DynamoDBQuery[In, Out] =
    self match {
      case zp @ ZipPar(left, right, zippable)     =>
        ZipPar(left.withRetryPolicy(policy), right.withRetryPolicy(policy), zippable)
      case b: BuilderNode[_, _]                   => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].withRetryPolicy(policy)
      case map @ DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.withRetryPolicy(policy), mapper)
      case ab @ DynamoDBQuery.Absolve(query)      =>
        DynamoDBQuery.Absolve(query.withRetryPolicy(policy))
      case bw: DynamoDBQuery.BatchWriteItem       => bw.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case bg: DynamoDBQuery.BatchGetItem         => bg.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case p: DynamoDBQuery.PutItem               => p.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem            => d.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case g: DynamoDBQuery.GetItem               => g.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem            => u.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.Scan                  => s.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: DynamoDBQuery.Query                 => q.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                      =>
        unsupportedModifier(
          "withRetryPolicy",
          "getItem/putItem/updateItem/deleteItem/query/scan/batchGetItem/batchWriteItem"
        )
    }

  /**
   * Sets the retry policy governing only the resubmission of `BatchGetItem` / `BatchWriteItem`'s
   * unprocessed keys / items, independent of `.withRetryPolicy`'s effect-level retry. Overrides
   * the interpreter's `defaultBatchUnprocessedRetryPolicy`; `RetryPolicy.NoRetry` turns the
   * resubmission off for this call. Fails with `UnsupportedModifier` on every other operation,
   * since only batch operations have unprocessed keys / items. See `docs/reference/retries.md`.
   */
  final def withBatchUnprocessedRetryPolicy(policy: RetryPolicy): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(left.withBatchUnprocessedRetryPolicy(policy), right.withBatchUnprocessedRetryPolicy(policy), zippable)
      case b: BuilderNode[_, _]             =>
        b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].withBatchUnprocessedRetryPolicy(policy)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.withBatchUnprocessedRetryPolicy(policy), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.withBatchUnprocessedRetryPolicy(policy))
      case bw: DynamoDBQuery.BatchWriteItem =>
        bw.copy(unprocessedRetryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case bg: DynamoDBQuery.BatchGetItem   =>
        bg.copy(unprocessedRetryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => unsupportedModifier("withBatchUnprocessedRetryPolicy", "batchGetItem/batchWriteItem")
    }

  /**
   * Filter a Scan or a Query
   */
  def filter[B](filterExpression: FilterExpression[B])(implicit ev: CanFilter[B, Out]): DynamoDBQuery[In, Out] = {
    val _ = ev
    self match {
      case zp @ ZipPar(left, right, zippable)     =>
        ZipPar(
          left.filter(filterExpression.asInstanceOf[FilterExpression[zp.Left]]),
          right.filter(filterExpression.asInstanceOf[FilterExpression[zp.Right]]),
          zippable
        )
      case b: BuilderNode[_, _]                   => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].filter(filterExpression)(ev)
      case map @ DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.filter(filterExpression.asInstanceOf[FilterExpression[map.Old]]), mapper)
      case ab @ DynamoDBQuery.Absolve(query)      =>
        DynamoDBQuery.Absolve(query.filter(filterExpression.asInstanceOf[FilterExpression[ab.Old]]))

      case s: DynamoDBQuery.Scan  =>
        s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.Query =>
        s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                      =>
        failUnsupported(
          s"'.filter' has no effect on ${self.getClass.getSimpleName}; filter expressions are only " +
            s"supported on scan/query — did you mean '.where'?"
        )
    }
  }

  final def indexName(indexName: String): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    => ZipPar(left.indexName(indexName), right.indexName(indexName), zippable)
      case b: BuilderNode[_, _]             => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].indexName(indexName)
      case DynamoDBQuery.Map(query, mapper) => DynamoDBQuery.Map(query.indexName(indexName), mapper)
      case DynamoDBQuery.Absolve(query)     => DynamoDBQuery.Absolve(query.indexName(indexName))
      case q: DynamoDBQuery.Scan            =>
        q.copy(indexName = Some(indexName)).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: DynamoDBQuery.Query           =>
        q.copy(indexName = Some(indexName)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => unsupportedModifier("indexName", "query/scan")
    }

  def capacity(capacity: ReturnConsumedCapacity): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)        =>
        ZipPar(left.capacity(capacity), right.capacity(capacity), zippable)
      case b: BuilderNode[_, _]                 => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].capacity(capacity)
      case DynamoDBQuery.Map(query, mapper)     =>
        DynamoDBQuery.Map(query.capacity(capacity), mapper)
      case DynamoDBQuery.Absolve(query)         =>
        DynamoDBQuery.Absolve(query.capacity(capacity))
      case g: DynamoDBQuery.GetItem             =>
        g.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case p: DynamoDBQuery.PutItem             =>
        p.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem          =>
        u.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem          =>
        d.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.Scan                =>
        s.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: DynamoDBQuery.Query               =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case bg: DynamoDBQuery.BatchGetItem       =>
        bg.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case bw: DynamoDBQuery.BatchWriteItem     =>
        bw.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case tg: DynamoDBQuery.TransactGetItems   =>
        tg.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case tw: DynamoDBQuery.TransactWriteItems =>
        tw.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                    =>
        unsupportedModifier(
          "capacity",
          "getItem/putItem/updateItem/deleteItem/query/scan/batchGetItem/batchWriteItem/transactGetItems/transactWriteItems"
        )
    }

  final def consistency(consistency: ConsistencyMode): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(left.consistency(consistency), right.consistency(consistency), zippable)
      case b: BuilderNode[_, _]             => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].consistency(consistency)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.consistency(consistency), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.consistency(consistency))
      case g: DynamoDBQuery.GetItem         =>
        g.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.Scan            =>
        s.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: DynamoDBQuery.Query           =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => unsupportedModifier("consistency", "getItem/query/scan")
    }

  private[dynamodb] final def returns(returnValues: ReturnValues): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(left.returns(returnValues), right.returns(returnValues), zippable)
      case b: BuilderNode[_, _]             => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].returns(returnValues)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.returns(returnValues), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.returns(returnValues))
      case p: DynamoDBQuery.PutItem         =>
        p.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem      =>
        u.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem      =>
        d.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => unsupportedModifier("returns", "putItem/updateItem/deleteItem")
    }

  def metrics(itemMetrics: ReturnItemCollectionMetrics): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)        =>
        ZipPar(left.metrics(itemMetrics), right.metrics(itemMetrics), zippable)
      case b: BuilderNode[_, _]                 => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].metrics(itemMetrics)
      case DynamoDBQuery.Map(query, mapper)     =>
        DynamoDBQuery.Map(query.metrics(itemMetrics), mapper)
      case DynamoDBQuery.Absolve(query)         =>
        DynamoDBQuery.Absolve(query.metrics(itemMetrics))
      case p: DynamoDBQuery.PutItem             =>
        p.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem          =>
        u.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem          =>
        d.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case bw: DynamoDBQuery.BatchWriteItem     =>
        bw.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case tw: DynamoDBQuery.TransactWriteItems =>
        tw.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                    => unsupportedModifier("metrics", "putItem/updateItem/deleteItem/batchWriteItem/transactWriteItems")
    }

  def startKey(exclusiveStartKey: LastEvaluatedKey): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(left.startKey(exclusiveStartKey), right.startKey(exclusiveStartKey), zippable)
      case b: BuilderNode[_, _]                        => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].startKey(exclusiveStartKey)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.startKey(exclusiveStartKey), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.startKey(exclusiveStartKey))
      case s: DynamoDBQuery.Scan                       =>
        s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.Query                      =>
        s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => unsupportedModifier("startKey", "query/scan")
    }

  def sortOrder(ascending: Boolean): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    => ZipPar(left.sortOrder(ascending), right.sortOrder(ascending), zippable)
      case q: DynamoDBQuery.Query           => q.copy(ascending = ascending).asInstanceOf[DynamoDBQuery[In, Out]]
      case b: BuilderNode[_, _]             => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].sortOrder(ascending)
      case DynamoDBQuery.Map(query, mapper) => DynamoDBQuery.Map(query.sortOrder(ascending), mapper)
      case DynamoDBQuery.Absolve(query)     => DynamoDBQuery.Absolve(query.sortOrder(ascending))
      case _                                => unsupportedModifier("sortOrder", "query")
    }

  def segment(index: Int, total: Int): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    => ZipPar(left.segment(index, total), right.segment(index, total), zippable)
      case b: BuilderNode[_, _]             => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].segment(index, total)
      case DynamoDBQuery.Map(query, mapper) => DynamoDBQuery.Map(query.segment(index, total), mapper)
      case DynamoDBQuery.Absolve(query)     => DynamoDBQuery.Absolve(query.segment(index, total))
      case s: DynamoDBQuery.Scan            =>
        s.copy(segment = index, totalSegments = total).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => unsupportedModifier("segment", "scan")
    }

  def gsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType,
    readCapacityUnit: Long,
    writeCapacityUnit: Long
  ): DynamoDBQuery[In, Out] =

    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(
          left.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit),
          right.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit),
          zippable
        )
      case b: BuilderNode[_, _]                        =>
        b.asQuery
          .asInstanceOf[DynamoDBQuery[In, Out]]
          .gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit)
      case DynamoDBQuery.Map(query, mapper)            =>
        DynamoDBQuery.Map(query.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit), mapper)
      case DynamoDBQuery.Absolve(query)                =>
        DynamoDBQuery.Absolve(query.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit))
      case s: DynamoDBQuery.CreateTable                =>
        s.copy(globalSecondaryIndexes =
          s.globalSecondaryIndexes + GlobalSecondaryIndex(
            indexName,
            keySchema,
            projection,
            Some(ProvisionedThroughput(readCapacityUnit, writeCapacityUnit))
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => unsupportedModifier("gsi", "createTable")
    }

  def gsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType
  ): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(
          left.gsi(indexName, keySchema, projection),
          right.gsi(indexName, keySchema, projection),
          zippable
        )
      case b: BuilderNode[_, _]                        => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].gsi(indexName, keySchema, projection)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.gsi(indexName, keySchema, projection), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.gsi(indexName, keySchema, projection))
      case s: DynamoDBQuery.CreateTable                =>
        s.copy(globalSecondaryIndexes =
          s.globalSecondaryIndexes + GlobalSecondaryIndex(
            indexName,
            keySchema,
            projection,
            None
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => unsupportedModifier("gsi", "createTable")
    }

  def lsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType = ProjectionType.All
  ): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(
          left.lsi(indexName, keySchema, projection),
          right.lsi(indexName, keySchema, projection),
          zippable
        )
      case b: BuilderNode[_, _]                        => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].lsi(indexName, keySchema, projection)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.lsi(indexName, keySchema, projection), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.lsi(indexName, keySchema, projection))
      case s: DynamoDBQuery.CreateTable                =>
        s.copy(localSecondaryIndexes =
          s.localSecondaryIndexes + LocalSecondaryIndex(
            indexName,
            keySchema,
            projection
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => unsupportedModifier("lsi", "createTable")
    }

  def selectAllAttributes: DynamoDBQuery[In, Out]          = select(Select.AllAttributes, "selectAllAttributes")
  def selectAllProjectedAttributes: DynamoDBQuery[In, Out] =
    select(Select.AllProjectedAttributes, "selectAllProjectedAttributes")
  def selectSpecificAttributes: DynamoDBQuery[In, Out]     = select(Select.SpecificAttributes, "selectSpecificAttributes")
  def selectCount: DynamoDBQuery[In, Out]                  = select(Select.Count, "selectCount")

  private def select(select: Select, modifier: String): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(left.select(select, modifier), right.select(select, modifier), zippable)
      case b: BuilderNode[_, _]                        => b.asQuery.asInstanceOf[DynamoDBQuery[In, Out]].select(select, modifier)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.select(select, modifier), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.select(select, modifier))
      case s: DynamoDBQuery.Scan                       => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.Query                      => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => unsupportedModifier(modifier, "query/scan")
    }

  // A modifier on an operation it doesn't affect fails the query when it runs instead of being dropped.
  // A Fail keeps its own error, and a Succeed sends no request for a modifier to affect.
  private def failUnsupported(message: => String): DynamoDBQuery[In, Out] =
    self match {
      case _: DynamoDBQuery.Fail | _: DynamoDBQuery.Succeed[_] => self
      case _                                                   =>
        DynamoDBQuery.fail(DynamoDBError.QueryBuilderError.UnsupportedModifier(message))
    }

  private def unsupportedModifier(modifier: String, supportedOn: String): DynamoDBQuery[In, Out] =
    failUnsupported(
      s"'.$modifier' has no effect on ${self.getClass.getSimpleName}; it is only supported on $supportedOn"
    )

}

/**
 * Factory methods for every Low-Level constructor (`getItem`, `putItem`, `updateItem`,
 * `deleteItem`, `query`, `scan`, `batchGetItem`, `batchWriteItem`,
 * `transactGetItems`, `transactWriteItems`, `createTable`, `deleteTable`, `describeTable`)
 * and the ADT case classes each one builds. See the "CRUD Operations" page on the project
 * docs site for the full Low-Level/High-Level/AWS-SDK operation matrix.
 */
object DynamoDBQuery {

  private[dynamodb] val itemOnFailure: Option[ReturnValuesOnConditionCheckFailure] = Some(
    ReturnValuesOnConditionCheckFailure.AllOld
  )
  import scala.collection.immutable.{ Map => ScalaMap, Set => ScalaSet }

  def succeed[A](a: => A): DynamoDBQuery[Any, A] = Succeed(() => a)

  def fail(e: => DynamoDBError): DynamoDBQuery[Any, Nothing] = Fail(() => e)

  private[dynamodb] final case class ZipPar[A, B, C](
    left: DynamoDBQuery[_, A],
    right: DynamoDBQuery[_, B],
    zippable: Zippable.Out[A, B, C]
  ) extends DynamoDBQuery[Any, C] {
    type Left  = A
    type Right = B
  }

  private[dynamodb] final case class Map[A, B](query: DynamoDBQuery[_, A], mapper: A => B)
      extends DynamoDBQuery[Any, B] {
    type Old = A
  }

  private[dynamodb] final case class Fail(error: () => DynamoDBError) extends Constructor[Any, Nothing]

  private[dynamodb] final case class Absolve[A, B](query: DynamoDBQuery[A, Either[ItemError, B]])
      extends DynamoDBQuery[A, B] {
    type Old = Either[ItemError, B]
  }

  def apply[A](a: => A): DynamoDBQuery[Any, A] = Succeed(() => a)

  private[dynamodb] final case class Succeed[A](value: () => A) extends Constructor[Any, A]

  // `body`'s type doesn't restrict it to a get, so anything else is rejected here, while building;
  // a failed query (e.g. a modifier a get doesn't support) throws its own, more specific error.
  def batchGetItem[In, A, B](values: Iterable[A])(body: A => DynamoDBQuery[In, B]): DynamoDBQuery.BatchGetItem =
    values.foldLeft(BatchGetItem()) { (batch, a) =>
      body(a) match {
        case g: GetItem => batch + g
        case f: Fail    => throw f.error()
        case other      =>
          throw DynamoDBError.QueryBuilderError.UnbatchableQuery(
            s"batchGetItem's callback must return a getItem, got ${other.getClass.getSimpleName}"
          )
      }
    }

  def batchWriteItem[A](values: Iterable[A])(
    body: A => ItemWriteBuilder[Any, Unit, _ <: WriteOp.Batchable]
  ): DynamoDBQuery.BatchWriteItem =
    values.foldLeft(BatchWriteItem()) { (batch, a) =>
      batch + body(a)
    }

  sealed trait Constructor[-In, +A] extends DynamoDBQuery[In, A]

  sealed trait Write[-In, +A] extends Constructor[In, A]

  final case class GetItem(
    tableName: String,
    key: PrimaryKey,
    projections: List[ProjectionExpression[_, _]] =
      List.empty, // If no attribute names are specified, then all attributes are returned,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    retryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, Option[Item]]

  private[dynamodb] final case class PutItem(
    tableName: String,
    item: Item,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None, // PutItem does not recognize any values other than NONE or ALL_OLD.
    retryPolicy: Option[RetryPolicy] = None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None
  ) extends Write[Any, Option[Item]]

  private[dynamodb] final case class UpdateItem(
    tableName: String,
    key: PrimaryKey,
    updateExpression: UpdateExpression[_],
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None,
    retryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, Option[Item]]

  private[dynamodb] final case class DeleteItem(
    tableName: String,
    key: PrimaryKey,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues =
      ReturnValues.None, // DeleteItem does not recognize any values other than NONE or ALL_OLD.
    retryPolicy: Option[RetryPolicy] = None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None
  ) extends Write[Any, Option[Item]]

  private[dynamodb] final case class ConditionCheck(
    tableName: String,
    key: PrimaryKey,
    conditionExpression: ConditionExpression[_],
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None
  ) extends Constructor[Any, Unit] {

    private[dynamodb] def withoutItemOnConditionFailure: ConditionCheck =
      copy(returnValuesOnConditionCheckFailure = Some(ReturnValuesOnConditionCheckFailure.None))
  }

  private[dynamodb] final case class TransactGetItems(
    getItems: Chunk[GetItem],
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends Constructor[Any, Chunk[Option[Item]]]

  private[dynamodb] final case class TransactWriteItems(
    writeItems: Chunk[DynamoDBQuery[Any, Any]],
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    clientRequestToken: Option[String] = None
  ) extends Constructor[Any, Unit]

  private[dynamodb] final case class Scan(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                              // if ProjectExpression supplied then only valid value is SpecificAttributes
    segment: Int = 0,
    totalSegments: Int = 1,
    retryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, Page[Item]]

  private[dynamodb] final case class Query(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    keyConditionExpr: Option[KeyConditionExpr[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                              // if ProjectExpression supplied then only valid value is SpecificAttributes
    ascending: Boolean = true,
    retryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, Page[Item]]

  private[dynamodb] final case class CreateTable(
    tableName: String,
    keySchema: KeySchema,
    attributeDefinitions: NonEmptySet[AttributeDefinition],
    billingMode: BillingMode,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty // you can have up to 50 tags
  ) extends Constructor[Any, Unit]
  private[dynamodb] final case class DeleteTable(
    tableName: String
  ) extends Constructor[Any, Unit]
  private[dynamodb] final case class DescribeTable(
    tableName: String
  ) extends Constructor[Any, DescribeTableResponse]
  // TODO: (adam) Add more fields here, this was for some basic testing initially
  final case class DescribeTableResponse(
    tableArn: String,
    tableStatus: TableStatus,
    tableSizeBytes: Long,
    itemCount: Long
  ) {
    override def toString: String =
      s"tableArn: $tableArn, tableStatus: $tableStatus, tableSizeBytes: $tableSizeBytes, itemCount: $itemCount"
  }
  sealed trait TableStatus
  object TableStatus {
    case object Creating                          extends TableStatus
    case object Updating                          extends TableStatus
    case object Deleting                          extends TableStatus
    case object Active                            extends TableStatus
    case object InaccessibleEncryptionCredentials extends TableStatus
    case object Archiving                         extends TableStatus
    case object Archived                          extends TableStatus
    case object ReplicationNotAuthorized          extends TableStatus
    case object unknownToSdkVersion               extends TableStatus
  }

  private[dynamodb] final case class BatchGetItem(
    requestItems: ScalaMap[String, BatchGetItem.TableGet] = ScalaMap.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    private[dynamodb] val orderedGetItems: Chunk[GetItem] =
      Chunk.empty, // track order of added GetItems for later unpacking
    retryPolicy: Option[RetryPolicy] = None,
    // Governs only the batch unprocessed retry — independent of
    // `retryPolicy`, which governs the effect-level loop. See `.withBatchUnprocessedRetryPolicy`.
    unprocessedRetryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, Batch.GetResult] { self =>

    def +(getItem: GetItem): BatchGetItem = {
      val tableName                                                     = getItem.tableName
      val key                                                           = getItem.key
      val projectionExpressionSet: ScalaSet[ProjectionExpression[_, _]] = getItem.projections.toSet
      val newEntry: (String, BatchGetItem.TableGet)                     =
        self.requestItems
          .get(tableName)
          .fold((tableName, BatchGetItem.TableGet(ScalaSet(key), getItem.projections.toSet)))(t =>
            (
              tableName,
              BatchGetItem.TableGet(t.keysSet + key, t.projectionExpressionSet ++ projectionExpressionSet)
            )
          )
      BatchGetItem(
        self.requestItems + newEntry,
        self.capacity,
        self.orderedGetItems :+ getItem,
        self.retryPolicy.orElse(getItem.retryPolicy), // inherit retry policy from GetItem if not set
        self.unprocessedRetryPolicy
      )
    }

    def addAll(entries: GetItem*): BatchGetItem =
      entries.foldLeft(self) { case (batch, getItem) =>
        batch + getItem
      }

    /*
     for each added GetItem, check it's key exists in the response and create a corresponding Optional Item value
     */
    def toGetItemResponses(response: BatchGetItem.Response): Chunk[Option[Item]] = {
      val chunk: Chunk[Option[Item]] = orderedGetItems.foldLeft[Chunk[Option[Item]]](Chunk.empty) {
        case (chunk, getItem) =>
          val responsesForTable: Chunk[Item] = response.responses.getOrElse(getItem.tableName, Chunk.empty[Item])
          // What if the projection expression for responsesForTable doesn't include the primaryKey?
          // Shouldn't the responseForTable have only the requested item?
          val found: Option[Item]            = responsesForTable.find { item =>
            getItem.key.map.toSet.subsetOf(item.map.toSet)
          }
          found.fold(chunk :+ None)(item => chunk :+ Some(item))
      }

      chunk
    }

  }
  private[dynamodb] object BatchGetItem {
    final case class TableGet(
      keysSet: ScalaSet[PrimaryKey],
      projectionExpressionSet: ScalaSet[ProjectionExpression[_, _]]
    )
    final case class Response(
      // Note - if a requested item does not exist, it is not returned in the result
      responses: ScalaMap[String, Chunk[Item]] = ScalaMap.empty,
      unprocessedKeys: ScalaMap[String, TableGet] = ScalaMap.empty
    )
  }

  private[dynamodb] final case class BatchWriteItem(
    requestItems: ScalaMap[String, Chunk[BatchWriteItem.Write]] = ScalaMap.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    addList: Chunk[BatchWriteItem.Write] = Chunk.empty,
    retryPolicy: Option[RetryPolicy] = None,
    // Governs only the batch unprocessed retry — independent of
    // `retryPolicy`, which governs the effect-level loop. See `.withBatchUnprocessedRetryPolicy`.
    unprocessedRetryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, Batch.WriteResult] { self =>
    // Only a plain put / delete builder is accepted (no condition, no ReturnValues - a batch supports
    // neither), and those always hold a PutItem / DeleteItem node, so the cast to Write is safe.
    def +(writeItem: ItemWriteBuilder[Any, Unit, _ <: WriteOp.Batchable]): BatchWriteItem =
      add(writeItem.write.asInstanceOf[Write[Any, Option[Item]]])

    def addAll(entries: ItemWriteBuilder[Any, Unit, _ <: WriteOp.Batchable]*): BatchWriteItem =
      entries.foldLeft(self)(_ + _)

    private def add(writeItem: Write[Any, Option[Item]]): BatchWriteItem =
      writeItem match {
        case putItem @ PutItem(_, _, _, _, _, _, _, _)       =>
          val write = Put(putItem.item)
          BatchWriteItem(
            self.requestItems
              .updated(putItem.tableName, self.requestItems.getOrElse(putItem.tableName, Chunk.empty) :+ write),
            self.capacity,
            self.itemMetrics,
            self.addList :+ write,
            self.retryPolicy.orElse(putItem.retryPolicy), // inherit retry policy from PutItem if not set
            self.unprocessedRetryPolicy
          )
        case deleteItem @ DeleteItem(_, _, _, _, _, _, _, _) =>
          val write = Delete(deleteItem.key)
          BatchWriteItem(
            self.requestItems
              .updated(deleteItem.tableName, self.requestItems.getOrElse(deleteItem.tableName, Chunk.empty) :+ write),
            self.capacity,
            self.itemMetrics,
            self.addList :+ write,
            self.retryPolicy.orElse(deleteItem.retryPolicy), // inherit retry policy from DeleteItem if not set
            self.unprocessedRetryPolicy
          )
      }

  }

  private[dynamodb] object BatchWriteItem {
    sealed trait Write
    final case class Delete(key: PrimaryKey) extends Write
    final case class Put(item: Item)         extends Write

    final case class Response(
      unprocessedItems: Option[ScalaMap[String, Chunk[BatchWriteItem.Write]]]
    )

  }

  def putItem(tableName: String, item: Item): ItemWriteBuilder[Any, Unit, WriteOp.Put] =
    new ItemWriteBuilder(PutItem(tableName, item), WriteBuilder.discard)

  /**
   * Never retries automatically, unlike every other operation — `action` may be a delta
   * (`.add`/`.increment`/`.decrement`/`.appendList`/`.prependList`), and retrying one after an
   * ambiguous-outcome failure can double-apply it. Attach a policy explicitly via
   * `.withRetryPolicy(...)` only when `action` is idempotent (`.set`/`.remove`/`.deleteFromSet`).
   */
  def updateItem[A](tableName: String, key: PrimaryKey)(action: Action[A]): ItemWriteBuilder[A, Unit, WriteOp.Update] =
    new ItemWriteBuilder(UpdateItem(tableName, key, UpdateExpression(action)), WriteBuilder.discard)

  /** @see the other `updateItem` overload — same retry caveat applies to `updateExpression`. */
  def updateItem(
    tableName: String,
    key: PrimaryKey,
    updateExpression: UpdateExpression[_]
  ): ItemWriteBuilder[Any, Unit, WriteOp.Update] =
    new ItemWriteBuilder(UpdateItem(tableName, key, updateExpression), WriteBuilder.discard)

  def getItem(
    tableName: String,
    key: PrimaryKey,
    projections: ProjectionExpression[_, _]*
  ): DynamoDBQuery[Any, Option[Item]] =
    GetItem(tableName, key, projections.toList)

  def deleteItem(tableName: String, key: PrimaryKey): ItemWriteBuilder[Any, Unit, WriteOp.Delete] =
    new ItemWriteBuilder(DeleteItem(tableName, key), WriteBuilder.discard)

  def createTable(
    tableName: String,
    keySchema: KeySchema,
    attributeDefinition: AttributeDefinition,
    attributeDefinitions: AttributeDefinition*
  )(
    billingMode: BillingMode,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty
  ): DynamoDBQuery[Any, Unit] =
    CreateTable(
      tableName,
      keySchema,
      NonEmptySet(attributeDefinition, attributeDefinitions: _*), // do not expose NonEmptySet publicly
      billingMode,
      globalSecondaryIndexes,
      localSecondaryIndexes,
      sseSpecification,
      tags
    )

  def deleteTable(tableName: String): DynamoDBQuery[Any, Unit] =
    DeleteTable(tableName)

  def describeTable(tableName: String): DynamoDBQuery[Any, DescribeTableResponse] =
    DescribeTable(tableName)

  def query(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey = None,
    ascending: Boolean = true
  ): DynamoDBQuery[Any, Page[Item]] =
    Query(tableName, limit, indexName, consistency, exclusiveStartKey, ascending = ascending)

  def scan(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey = None,
    filterExpression: Option[FilterExpression[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None
  ): DynamoDBQuery[Any, Page[Item]] =
    Scan(
      tableName,
      limit,
      indexName,
      consistency,
      exclusiveStartKey,
      filterExpression,
      projections,
      capacity,
      select
    )

  def transactGetItems(items: GetItem*): DynamoDBQuery[Any, Chunk[Option[Item]]] =
    TransactGetItems(Chunk.fromIterable(items))

  // Wildcard on In too (not fixed to Any) so a High-Level put/update/deleteFrom/conditionCheck
  // value (DynamoDBQuery[SomeModel, _]) is accepted alongside Low-Level constructors. In is a
  // phantom type in this ADT — nothing at runtime reads it — so widening the declared bound
  // doesn't admit anything unsound; runAny unwraps each item to its real Write/ConditionCheck
  // leaf regardless of what In each one carries.
  def transactWriteItems(items: DynamoDBQuery[_, _]*): DynamoDBQuery[Any, Unit] =
    TransactWriteItems(Chunk.fromIterable(items.map(_.asInstanceOf[DynamoDBQuery[Any, Any]])))

  def conditionCheck(
    tableName: String,
    primaryKey: PrimaryKey
  )(conditionExpression: ConditionExpression[_]): ConditionCheckBuilder[Any] =
    new ConditionCheckBuilder(
      ConditionCheck(
        tableName,
        primaryKey,
        conditionExpression,
        returnValuesOnConditionCheckFailure = Some(ReturnValuesOnConditionCheckFailure.AllOld)
      )
    )

}

// -- Builders ------------------------------------------------------------
// Builders are themselves DynamoDBQuery cases, so they run and compose without a conversion. Each
// has an equivalent plain query (`asQuery`) that the general DynamoDBQuery modifiers delegate to when
// a builder is reached through one (e.g. nested in zipPar) and doesn't override the modifier itself.
sealed abstract class BuilderNode[In, Out] extends DynamoDBQuery[In, Out] {
  private[dynamodb] def asQuery: DynamoDBQuery[In, Out]
}

// A write builder: `write` is the put / update / delete node and `finish` maps its raw result, so it
// runs exactly like `Map(write, finish)` without allocating one. The concrete builders (Low-Level here
// in core, High-Level in schema-ddbexpr) extend WriteBuilder / ConditionalWriteBuilder; nothing here
// depends on them.
sealed abstract class WriteNode[In, Out] private[dynamodb] (
  private[dynamodb] val write: DynamoDBQuery[In, Option[Item]],
  private[dynamodb] val finish: Option[Item] => Out
) extends BuilderNode[In, Out] {
  private[dynamodb] def asQuery: DynamoDBQuery[In, Out] = DynamoDBQuery.Map(write, finish)
}

/**
 * A put / update / delete that executes to `Out` - `Unit` unless the concrete builder's `.returning`
 * asks for the item DynamoDB sends back. `.where` makes it a
 * [[ConditionalWriteBuilder]]. Itself a [[DynamoDBQuery]], so it runs, composes (`zipPar`,
 * `transactWriteItems`) and takes the general modifiers directly; the concrete builders override
 * `capacity` / `metrics` / `withRetryPolicy` to keep the builder, so they compose with `.where` /
 * `.returning` in any order.
 */
abstract class WriteBuilder[In, Out] private[dynamodb] (
  write0: DynamoDBQuery[In, Option[Item]],
  finish0: Option[Item] => Out
) extends WriteNode[In, Out](write0, finish0) {

  /**
   * Makes the write conditional. A failed condition fails the effect with
   * [[DynamoDBError.ItemError.ConditionFailed]], whose `item` holds the item as it was -
   * AWS's `ReturnValuesOnConditionCheckFailure=ALL_OLD` is sent by default (no read capacity is consumed;
   * only the failure response is larger). Turn it off with
   * [[ConditionalWriteBuilder.withoutItemOnConditionFailure]].
   */
  def where(ce: ConditionExpression[In]): ConditionalWriteBuilder[In, Out]

  def toQuery: DynamoDBQuery[In, Out] = this

  def execute[F[_]](implicit interpreter: Interpreter[F]): F[Out] =
    interpreter.run(this)
}

private[dynamodb] object WriteBuilder {
  val discard: Option[Item] => Unit = _ => ()

  def conditional[In](
    write: DynamoDBQuery[In, Option[Item]],
    ce: ConditionExpression[_]
  ): DynamoDBQuery[In, Option[Item]] =
    write.withConditionReturningItemOnFailure(ce)

  def withoutItemOnConditionFailure[In](write: DynamoDBQuery[In, Option[Item]]): DynamoDBQuery[In, Option[Item]] =
    write.returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.None)
}

/**
 * A write with a condition. A failed condition fails the effect with
 * [[DynamoDBError.ItemError.ConditionFailed]], whose `item` is the item as it was (returned by
 * default - see [[withoutItemOnConditionFailure]]); every other failure is the underlying AWS SDK
 * exception.
 */
abstract class ConditionalWriteBuilder[In, Out] private[dynamodb] (
  write0: DynamoDBQuery[In, Option[Item]],
  finish0: Option[Item] => Out
) extends WriteNode[In, Out](write0, finish0) {

  /** Replaces the condition. */
  def where(ce: ConditionExpression[In]): ConditionalWriteBuilder[In, Out]

  /**
   * Stops a failed condition from returning the existing item (`ConditionFailed.item` is then
   * `None`): for very large items or frequently failing conditions, where the larger failure
   * response isn't wanted.
   */
  def withoutItemOnConditionFailure: ConditionalWriteBuilder[In, Out]

  def toQuery: DynamoDBQuery[In, Out] = this

  def execute[F[_]](implicit interpreter: Interpreter[F]): F[Out] =
    interpreter.run(this)
}

/**
 * Returned by `conditionCheck` (Low-Level and High-Level): a condition-only transaction action. A
 * failed check returns the item as it was on its `CancellationReason` by default;
 * `.withoutItemOnConditionFailure` turns that off. The general modifiers keep the builder, so the
 * opt-out is available in any order.
 */
final class ConditionCheckBuilder[In] private[dynamodb] (private[dynamodb] val check: DynamoDBQuery[In, Unit])
    extends BuilderNode[In, Unit] {
  private[dynamodb] def asQuery: DynamoDBQuery[In, Unit] = check

  /** For very large items or frequently failing checks, where the item isn't wanted on failure. */
  def withoutItemOnConditionFailure: ConditionCheckBuilder[In] =
    new ConditionCheckBuilder(check match {
      case c: DynamoDBQuery.ConditionCheck => c.withoutItemOnConditionFailure
      case other                           => other // e.g. a High-Level key that didn't resolve
    })

  override def capacity(capacity: ReturnConsumedCapacity): ConditionCheckBuilder[In]        =
    new ConditionCheckBuilder(check.capacity(capacity))
  override def metrics(itemMetrics: ReturnItemCollectionMetrics): ConditionCheckBuilder[In] =
    new ConditionCheckBuilder(check.metrics(itemMetrics))
  override def withRetryPolicy(policy: RetryPolicy): ConditionCheckBuilder[In]              =
    new ConditionCheckBuilder(check.withRetryPolicy(policy))
}
