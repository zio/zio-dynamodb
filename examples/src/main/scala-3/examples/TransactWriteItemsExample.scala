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

package examples

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.{ Task, ZIO, ZIOAppDefault, ZLayer }
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ DynamoDBQuery, Interpreter, ZioInterpreter }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

/**
 * Scala 3 / ZIO showcase for `transactWriteItems` accepting High-Level values directly (see
 * [Transactions](../../../../../docs/reference/crud/transactions.md)): `put`/`update`/
 * `conditionCheck` compose into the same call exactly like any other High-Level query. See
 * [[TransactWriteItemsLowLevelExample]] for the same transaction built from Low-Level
 * constructors instead. Not run against a real client (no Docker/Testcontainers dependency);
 * a method body type-checks whether or not it's ever called, so this fails `examples/compile`
 * if the pattern stops compiling.
 */
object TransactWriteItemsExample extends ZIOAppDefault {

  case class Account(id: String, balance: Int, status: String) derives Schema

  object Account extends CompanionOptics[Account] {
    val id: Lens[Account, String]     = $(_.id)
    val balance: Lens[Account, Int]   = $(_.balance)
    val status: Lens[Account, String] = $(_.status)
  }

  val accounts = Table[Account]("accounts")

  val interpreterLayer: ZLayer[Any, Throwable, Interpreter[Task]] =
    ZLayer.scoped {
      ZIO
        .acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        .map(client => ZioInterpreter.fromAsyncClient(client): Interpreter[Task])
    }

  val program: ZIO[Interpreter[Task], Throwable, Unit] =
    ZIO.serviceWithZIO[Interpreter[Task]] { interpreter =>
      given Interpreter[Task] = interpreter
      for {
        _ <- put(accounts, Account("checking", 500, "open")).execute
        _ <- put(accounts, Account("savings", 0, "open")).execute
        _ <- put(accounts, Account("escrow", 0, "open")).execute

        // One atomic transaction, entirely from High-Level values. DynamoDB rejects a
        // transaction where two actions target the same item (e.g. a conditionCheck and an
        // update on the same account) — conditionCheck guards a different item here (escrow)
        // than the ones being written; a condition on an item you ARE writing belongs on that
        // same action instead, via `.where(...)`.
        _ <- DynamoDBQuery
               .transactWriteItems(
                 conditionCheck(accounts)(Account.id.partitionKey === "escrow")(Account.status === "open"),
                 update(accounts)(Account.id.partitionKey === "checking")(Account.balance.set(400)),
                 update(accounts)(Account.id.partitionKey === "savings")(Account.balance.set(100)),
                 deleteFrom(accounts)(Account.id.partitionKey === "stale")
               )
               .execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}
