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

import zio.Task
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{
  DynamoDBError,
  DynamoDBQuery,
  Interpreter,
  RetryPolicy,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  ReturnValues
}
import zio.dynamodb.DynamoDBError.CancellationCode
import zio.dynamodb.DynamoDBError.ItemError.ConditionFailed
import zio.dynamodb.DynamoDBError.TransactionError.TransactionCancelled
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.blocks.ddbexpr.dsl._

/**
 * High-Level writes beyond the plain CRUD calls: conditions, the item a failed condition returns,
 * getting a decoded item back with `.returning`, request options, and composing writes.
 *
 * `put` / `update` / `deleteFrom` execute to `Unit`. `.where` makes a write conditional; a failed
 * condition fails the effect with `ConditionFailed`, whose `item` is the item as it was (a raw
 * `Item`, sent by default - `.withoutItemOnConditionFailure` turns it off). `.returning` decodes
 * the returned item with the table's codec: `None` when nothing came back, `Left` when it doesn't
 * decode. `ConditionalWritesSpec` runs every query here.
 */
object ConditionalWrites {

  final case class Account(id: String, status: String, balance: Int)

  object Account extends CompanionOptics[Account] {
    implicit val schema: Schema[Account] = Schema.derived

    val id: Lens[Account, String]     = $(_.id)
    val status: Lens[Account, String] = $(_.status)
    val balance: Lens[Account, Int]   = $(_.balance)
  }

  val accounts: Table[Account] = Table[Account]("accounts")

  private type Returned = Option[Either[DynamoDBError.ItemError.DecodingError, Account]]

  // -- conditions ---------------------------------------------------------

  val putIfAbsent: DynamoDBQuery[Account, Unit] =
    put(accounts, Account("a1", "open", 0)).where(Account.id.attributeNotExists)

  val updateIfOpen: DynamoDBQuery[Account, Unit] =
    update(accounts)(Account.id.partitionKey === "a1")(Account.balance.add(10)).where(Account.status === "open")

  val deleteIfClosed: DynamoDBQuery[Account, Unit] =
    deleteFrom(accounts)(Account.id.partitionKey === "a1").where(Account.status === "closed")

  // For very large items or frequently failing conditions, skip the item in the failure response.
  val updateWithoutItemOnFailure: DynamoDBQuery[Account, Unit] =
    update(accounts)(Account.id.partitionKey === "a1")(Account.balance.add(10))
      .where(Account.status === "open")
      .withoutItemOnConditionFailure

  // -- getting a decoded item back -------------------------------------------

  val putReturningPrevious: DynamoDBQuery[Account, Returned] =
    put(accounts, Account("a1", "open", 5)).returning(ReturnValues.AllOld)

  val updateReturningNew: DynamoDBQuery[Account, Returned] =
    update(accounts)(Account.id.partitionKey === "a1")(Account.balance.add(10)).returning(ReturnValues.AllNew)

  val deleteReturningDeleted: DynamoDBQuery[Account, Returned] =
    deleteFrom(accounts)(Account.id.partitionKey === "a1")
      .where(Account.status === "closed")
      .returning(ReturnValues.AllOld)

  // -- request options, in any order with .where / .returning ---------------

  val withRequestOptions: DynamoDBQuery[Account, Returned] =
    update(accounts)(Account.id.partitionKey === "a1")(Account.balance.add(10))
      .capacity(ReturnConsumedCapacity.Total)
      .where(Account.status === "open")
      .metrics(ReturnItemCollectionMetrics.Size)
      .returning(ReturnValues.AllNew)
      .withRetryPolicy(RetryPolicy.NoRetry)

  // -- composing writes ---------------------------------------------------

  val twoWritesInParallel: DynamoDBQuery[Account, Unit] =
    put(accounts, Account("a2", "open", 0)) zipPar deleteFrom(accounts)(Account.id.partitionKey === "a1")

  // A conditionCheck guards the transaction without writing; it returns the item on failure by
  // default too.
  val guardedTransfer: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.transactWriteItems(
      conditionCheck(accounts)(Account.id.partitionKey === "a2")(Account.status === "open"),
      update(accounts)(Account.id.partitionKey === "a1")(Account.balance.add(-10)).where(Account.balance >= 10),
      update(accounts)(Account.id.partitionKey === "a2")(Account.balance.add(10))
    )

  // -- handling the expected failures (ZIO) ---------------------------------

  sealed trait Outcome
  case object Written                                           extends Outcome
  final case class Rejected(current: Option[zio.dynamodb.Item]) extends Outcome
  final case class Conflict(codes: List[String])                extends Outcome

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
