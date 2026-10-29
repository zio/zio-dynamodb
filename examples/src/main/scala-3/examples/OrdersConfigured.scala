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
import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, NameMapper, Schema }
import zio.dynamodb.{ Interpreter, ZioInterpreter }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver

/**
 * Scala 3 / ZIO example of codec configuration in the high-level API done entirely in code — the
 * model carries no `@Modifier` annotations. Derive the codec with ZIO Blocks' derivation builder
 * and pass it to `Table(name, codec)`:
 *
 *   - `DynamoDBCodecDeriver.withFieldNameMapper` / `withCaseNameMapper` — deriver-wide policy
 *   - `.modifier(Order.customerId, Modifier.rename(...))` — per-field wire name, addressed by
 *     optic (the value-level equivalent of `@Modifier.rename` on the model)
 *
 * With the codec below an `Order` is stored as:
 * {{{
 *   cust        (customerId — pinned by the rename, the field-name mapper is not applied)
 *   order_id    (orderId    — snake_case from withFieldNameMapper)
 *   total       (total)
 *   status      (status)    — value "pending" / "shipped" from withCaseNameMapper
 * }}}
 *
 * Not run against a real client (no Docker/Testcontainers dependency); a method body is
 * type-checked whether or not it is ever called, so this fails `examples/compile` if the
 * example stops compiling.
 */
object OrdersConfigured extends ZIOAppDefault {

  enum Status derives Schema {
    case Pending, Shipped
  }

  final case class Order(customerId: String, orderId: String, total: Double, status: Status) derives Schema

  object Order extends CompanionOptics[Order] {
    val customerId: Lens[Order, String] = $(_.customerId)
    val orderId: Lens[Order, String]    = $(_.orderId)
    val total: Lens[Order, Double]      = $(_.total)
    val status: Lens[Order, Status]     = $(_.status)
  }

  // Deriver-wide flags, then the per-field rename by optic.
  val orders: Table[Order] =
    Table(
      "orders",
      Schema[Order]
        .deriving(
          DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.SnakeCase).withCaseNameMapper(NameMapper.SnakeCase)
        )
        .modifier(Order.customerId, Modifier.rename("cust"))
        .derive
    )

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
        _ <- put(orders, Order("cust-42", "ord-1", 129.99, Status.Pending)).execute

        // every op on `orders` uses the codec the Table carries
        order <- get(orders)(
                   Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1"
                 ).execute

        // the filter literal (Status.Shipped) is encoded with that same codec
        shipped <- scan(orders, limit = 20).filter(Order.status === Status.Shipped).execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}
