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
import zio.blocks.chunk.Chunk

/**
 * A stateless [[ResponseInterceptor]] that logs a warning whenever a single call's consumed
 * capacity crosses a threshold — see the "Interceptor / Observability" reference doc for the
 * full walkthrough.
 */
object CapacityWarnings extends ZIOAppDefault {

  def totalUnits(consumed: Option[ConsumedCapacity]): Double =
    consumed.fold(0.0)(c => c.readCapacityUnits.getOrElse(0.0) + c.writeCapacityUnits.getOrElse(0.0))

  def totalUnitsBatch(consumed: Chunk[ConsumedCapacity]): Double =
    consumed.foldLeft(0.0)((acc, c) => acc + c.readCapacityUnits.getOrElse(0.0) + c.writeCapacityUnits.getOrElse(0.0))

  def capacityUnitsOf(meta: DynamoDBResponseMetadata): Double = meta match {
    case m: DynamoDBResponseMetadata.GetItem            => totalUnits(m.consumed)
    case m: DynamoDBResponseMetadata.PutItem            => totalUnits(m.consumed)
    case m: DynamoDBResponseMetadata.UpdateItem         => totalUnits(m.consumed)
    case m: DynamoDBResponseMetadata.DeleteItem         => totalUnits(m.consumed)
    case m: DynamoDBResponseMetadata.Query              => totalUnits(m.consumed)
    case m: DynamoDBResponseMetadata.Scan               => totalUnits(m.consumed)
    case m: DynamoDBResponseMetadata.BatchGetItem       => totalUnitsBatch(m.consumed)
    case m: DynamoDBResponseMetadata.BatchWriteItem     => totalUnitsBatch(m.consumed)
    case m: DynamoDBResponseMetadata.TransactGetItems   => totalUnitsBatch(m.consumed)
    case m: DynamoDBResponseMetadata.TransactWriteItems => totalUnitsBatch(m.consumed)
  }

  val capacityWarnings: ResponseInterceptor[Task] = new ResponseInterceptor[Task] {
    def onResponse(meta: DynamoDBResponseMetadata): Task[Unit] =
      ZIO.logWarning(s"expensive call: $meta").when(capacityUnitsOf(meta) > 10.0).unit
  }

  def run: Task[Unit] =
    ZIO.scoped {
      for {
        client <-
          ZIO.acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        interp = ZioInterpreter.fromAsyncClient(client, capacityWarnings)
        _      <- interp.run(DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")))
      } yield ()
    }
}
