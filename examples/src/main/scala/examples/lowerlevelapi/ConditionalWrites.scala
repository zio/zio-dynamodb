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

package examples.lowerlevelapi

import zio.Task
import zio.dynamodb.{
  DynamoDBQuery,
  Interpreter,
  Item,
  PrimaryKey,
  RetryPolicy,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  ReturnValues
}
import zio.dynamodb.DynamoDBError.CancellationCode
import zio.dynamodb.DynamoDBError.ItemError.ConditionFailed
import zio.dynamodb.DynamoDBError.TransactionError.TransactionCancelled
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.ProjectionExpression.$

/**
 * Low-Level writes beyond the plain CRUD calls: conditions, the item a failed condition returns,
 * getting an item back with `.returning`, request options, and composing writes.
 *
 * `putItem` / `updateItem` / `deleteItem` execute to `Unit`. `.where` makes a write conditional;
 * a failed condition fails the effect with `ConditionFailed`, whose `item` is the item as it was
 * (sent by default - `.withoutItemOnConditionFailure` turns it off). `ConditionalWritesSpec` runs
 * every query here.
 */
object ConditionalWrites {

  private val table = "accounts"
  private val key   = PrimaryKey("id" -> "a1")

  // -- conditions ---------------------------------------------------------

  val putIfAbsent: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.putItem(table, Item("id" -> "a1", "balance" -> 0)).where($("id").attributeNotExists)

  val updateIfOpen: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.updateItem(table, key)($("balance").add(10)).where($("status") === "open")

  val deleteIfClosed: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.deleteItem(table, key).where($("status") === "closed")

  // For very large items or frequently failing conditions, skip the item in the failure response.
  val updateWithoutItemOnFailure: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery
      .updateItem(table, key)($("balance").add(10))
      .where($("status") === "open")
      .withoutItemOnConditionFailure

  // -- getting an item back ------------------------------------------------

  val putReturningPrevious: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, Item("id" -> "a1", "balance" -> 5)).returning(ReturnValues.AllOld)

  val updateReturningNew: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)($("balance").add(10)).returning(ReturnValues.AllNew)

  val deleteReturningDeleted: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.deleteItem(table, key).where($("status") === "closed").returning(ReturnValues.AllOld)

  // -- request options, in any order with .where / .returning ---------------

  val withRequestOptions: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery
      .updateItem(table, key)($("balance").add(10))
      .capacity(ReturnConsumedCapacity.Total)
      .where($("status") === "open")
      .metrics(ReturnItemCollectionMetrics.Size)
      .returning(ReturnValues.UpdatedNew)
      .withRetryPolicy(RetryPolicy.NoRetry)

  // -- composing writes ---------------------------------------------------

  val twoWritesInParallel: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.putItem(table, Item("id" -> "a2")) zipPar DynamoDBQuery.deleteItem(table, key)

  // A conditionCheck guards the transaction without writing; it too returns the item on failure
  // by default, and has the same opt-out.
  val guardedTransfer: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.transactWriteItems(
      DynamoDBQuery.conditionCheck(table, PrimaryKey("id" -> "a2"))($("status") === "open"),
      DynamoDBQuery.updateItem(table, key)($("balance").add(-10)).where($("balance") >= 10),
      DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "a2"))($("balance").add(10))
    )

  val guardedTransferWithoutItems: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.transactWriteItems(
      DynamoDBQuery
        .conditionCheck(table, PrimaryKey("id" -> "a2"))($("status") === "open")
        .withoutItemOnConditionFailure,
      DynamoDBQuery.updateItem(table, key)($("balance").add(-10))
    )

  // -- handling the expected failures (ZIO) ---------------------------------

  sealed trait Outcome
  case object Written                              extends Outcome
  final case class Rejected(current: Option[Item]) extends Outcome
  final case class Conflict(codes: List[String])   extends Outcome

  // A narrow handler: only the failed condition is handled; every other failure (throttling,
  // timeouts, ...) passes through as the AWS SDK exception.
  def openAccount(implicit interpreter: Interpreter[Task]): Task[Outcome] =
    putIfAbsent.execute
      .as(Written: Outcome)
      .catchSome { case ConditionFailed(_, existing) => zio.ZIO.succeed(Rejected(existing)) }

  def transfer(implicit interpreter: Interpreter[Task]): Task[Outcome] =
    guardedTransfer.execute
      .as(Written: Outcome)
      .catchSome {
        case tc: TransactionCancelled if tc.reasons.exists(_.code == CancellationCode.ConditionalCheckFailed) =>
          zio.ZIO.succeed(Rejected(tc.reasons.flatMap(_.item).headOption))
        case tc: TransactionCancelled                                                                         =>
          zio.ZIO.succeed(Conflict(tc.reasons.map(_.code.value).toList))
      }

}
