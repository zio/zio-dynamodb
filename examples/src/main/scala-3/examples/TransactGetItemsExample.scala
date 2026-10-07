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
import zio.blocks.schema.Schema
import zio.dynamodb.{ DynamoDBQuery, Interpreter, PrimaryKey, ZioInterpreter }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

/**
 * Scala 3 / ZIO showcase for the Low-Level/High-Level split documented in
 * `docs/reference/crud/batch.md` ("Batch and the High-Level API"): a
 * homogeneous read transaction stays on the Low-Level `transactGetItems` constructor —
 * writing via the High-Level `put`, reading via `transactGetItems`, then decoding each raw
 * `Item` with `Table#decode` on the same `Table` `put` used, so the hand-rolled read path
 * stays consistent with the rest of the High-Level code. Not run against a real client (no
 * Docker/Testcontainers dependency); a method body type-checks whether or not it's ever
 * called, so this fails `examples/compile` if the pattern stops compiling.
 */
object TransactGetItemsExample extends ZIOAppDefault {

  case class Account(id: String, balance: Int) derives Schema

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
        _ <- put(accounts, Account("a1", 100)).execute
        _ <- put(accounts, Account("a2", 250)).execute

        // Homogeneous read transaction, deliberately Low-Level — no DdbExprApi wrapper
        // exists for transactGetItems, by design (see the doc reference above).
        _ <- DynamoDBQuery
               .transactGetItems(
                 DynamoDBQuery.GetItem(accounts.name, PrimaryKey("id" -> "a1")),
                 DynamoDBQuery.GetItem(accounts.name, PrimaryKey("id" -> "a2"))
               )
               .map(_.map(_.map(accounts.decode))) // Chunk[Option[Item]] -> Chunk[Option[Either[ItemError, Account]]]
               .execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}
