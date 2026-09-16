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
import zio._

/**
 * A stateful [[ResponseInterceptor]] that accumulates total consumed capacity across every
 * request in a session — one [[Ref]], no delays or windowing (contrast [[CapacityWarnings]],
 * which decides per-call and holds no state at all).
 */
object CapacityAccumulator extends ZIOAppDefault {

  def make: UIO[(ResponseInterceptor[Task], UIO[Double])] =
    Ref.make(0.0).map { total =>
      val interceptor = new ResponseInterceptor[Task] {
        def onResponse(meta: DynamoDBResponseMetadata): Task[Unit] =
          total.update(_ + CapacityWarnings.capacityUnitsOf(meta))
      }
      (interceptor, total.get)
    }

  def run: Task[Unit] =
    ZIO.scoped {
      for {
        pair   <- make
        (interceptor, totalConsumed) = pair
        client <-
          ZIO.acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        interp = ZioInterpreter.fromAsyncClient(client, interceptor)
        _      <- interp.run(DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")))
        _      <- interp.run(DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-2")))
        total  <- totalConsumed
        _      <- Console.printLine(s"total consumed capacity this session: $total")
      } yield ()
    }
}
