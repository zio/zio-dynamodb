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
import zio.dynamodb.{ DynamoDBQuery, Interpreter, Item, PrimaryKey, ZioInterpreter }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$

/**
 * Scala 3 / ZIO showcase for `transactWriteItems` built entirely from Low-Level constructors
 * — see [Transactions](../../../../../docs/reference/crud/transactions.md). See
 * [[TransactWriteItemsExample]] for the same shape of transaction built from High-Level
 * values instead. Not run against a real client (no Docker/Testcontainers dependency); a
 * method body type-checks whether or not it's ever called, so this fails `examples/compile`
 * if the pattern stops compiling.
 */
object TransactWriteItemsLowLevelExample extends ZIOAppDefault {

  val interpreterLayer: ZLayer[Any, Throwable, Interpreter[Task]] =
    ZLayer.scoped {
      ZIO
        .acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        .map(client => ZioInterpreter.fromAsyncClient(client): Interpreter[Task])
    }

  val program: ZIO[Interpreter[Task], Throwable, Unit] =
    ZIO.serviceWithZIO[Interpreter[Task]] { interpreter =>
      implicit val interp: Interpreter[Task] = interpreter
      for {
        _ <- DynamoDBQuery.putItem("accounts", Item("id" -> "checking", "balance" -> 500, "status" -> "open")).execute
        _ <- DynamoDBQuery.putItem("accounts", Item("id" -> "savings", "balance" -> 0, "status" -> "open")).execute

        // One atomic transaction, entirely from Low-Level constructors.
        _ <- DynamoDBQuery
               .transactWriteItems(
                 DynamoDBQuery.conditionCheck("accounts", PrimaryKey("id" -> "checking"))($("status") === "open"),
                 DynamoDBQuery.updateItem("accounts", PrimaryKey("id" -> "checking"))($("balance").set(400)),
                 DynamoDBQuery.updateItem("accounts", PrimaryKey("id" -> "savings"))($("balance").set(100)),
                 DynamoDBQuery.deleteItem("accounts", PrimaryKey("id" -> "stale"))
               )
               .execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}
