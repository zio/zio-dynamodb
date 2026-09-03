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

import cats.effect.{ IO, IOApp, Resource }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

/**
 * Scala 3 / Cats Effect showcase example, introducing the high-level API. Not run against a
 * real client (no Docker/Testcontainers dependency); a class/method body is type-checked
 * whether or not it's ever instantiated/called, so this fails `examples/compile` if the
 * example stops compiling. The underlying CEInterpreter + dsl facade + .execute mechanics
 * are already exercised for real elsewhere (CEDynamoDBSpec, CEHighLevelSpec).
 *
 * The client is `Resource`-managed rather than built and never closed — the resource's
 * release action runs `c.close()` once `use` completes, on both success and failure.
 */
object OrdersCE extends IOApp.Simple {

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

  // One Table handle per table — carries the name + Schema + codec config, and lets the
  // CRUD ops below infer their element type.
  val orders = Table[Order]("orders")

  val client: Resource[IO, DynamoDbAsyncClient] =
    Resource.make(IO(DynamoDbAsyncClient.builder().build()))(c => IO(c.close()))

  def run: IO[Unit] =
    client.use { c =>
      given Interpreter[IO] = CEInterpreter.fromAsyncClient(c)
      for {
        _ <- put(orders, Order("cust-42", "ord-1", 129.99, Status.Pending)).execute

        // partition key + sort-key range, plus a filter — both type-checked against
        // Order's schema, not hand-written expression strings
        recent <- query(orders, limit = 20)
                    .whereKey(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey > "ord-0")
                    .filter(Order.total > 50.0)
                    .execute

        // typed update — the compiler checks the field and the value being set together
        _ <- update(orders)(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1")(
               Order.status.set(Status.Shipped)
             ).execute
      } yield ()
    }
}
