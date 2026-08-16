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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.{ Task, ZIO, ZIOAppDefault, ZLayer }
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

/**
 * Scala 3 / ZIO showcase example — the ZIO counterpart of `OrdersCE.scala` (the Cats Effect
 * version) in this same module. Not run against a real client (no Docker/Testcontainers
 * dependency); a class/method body is type-checked whether or not it's ever
 * instantiated/called, so this fails `examples/compile` if the example stops compiling.
 *
 * The client is scope-managed via `ZLayer.scoped`/`ZIO.acquireRelease` rather than built
 * and never closed — the release action runs `c.close()` when the layer's scope ends.
 */
object OrdersZio extends ZIOAppDefault {

  enum Status derives Schema {
    case Pending, Shipped
  }

  case class Order(customerId: String, orderId: String, total: Double, status: Status) derives Schema

  object Order extends CompanionOptics[Order] {
    val customerId: Lens[Order, String] = $(_.customerId) // real optics
    val orderId: Lens[Order, String]    = $(_.orderId)
    val total: Lens[Order, Double]      = $(_.total)
    val status: Lens[Order, Status]     = $(_.status)
  }

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
        _ <- put("orders", Order("cust-42", "ord-1", 129.99, Status.Pending)).execute

        // partition key + sort-key range, plus a filter — both type-checked against
        // Order's schema, not hand-written expression strings
        recent <- query[Order]("orders", limit = 20)
                    .whereKey(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey > "ord-0")
                    .filter(Order.total > 50.0)
                    .execute

        // typed update — the compiler checks the field and the value being set together
        _ <- update("orders")(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1")(
               Order.status.set(Status.Shipped)
             ).execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}
