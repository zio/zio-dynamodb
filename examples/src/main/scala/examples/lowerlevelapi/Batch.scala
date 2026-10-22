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

import zio.dynamodb.{ Batch => DynamoDBBatch, ConsistencyMode, DynamoDBQuery, Item, PrimaryKey, RetryPolicy }

import zio.blocks.chunk.Chunk

import scala.concurrent.duration.DurationInt

/**
 * `batchGetItem`/`batchWriteItem` build one batch request from a `Iterable[A]` plus a function
 * from each `A` to a single-item `getItem`/`putItem`/`deleteItem` query — there's no per-model
 * derivation involved, `A` can be anything the caller has on hand (here, a plain order ID).
 */
object Batch {

  private val table = "orders"

  private val orderIds: List[String] = List("o1", "o2")

  // Every batch below resubmits its unprocessed keys / items by default (RetryPolicy.fullJitter()).
  // .withBatchUnprocessedRetryPolicy overrides that per call; RetryPolicy.NoRetry returns Incomplete instead.
  val batchGet: DynamoDBQuery[Any, DynamoDBBatch.GetResult] =
    DynamoDBQuery
      .batchGetItem(orderIds) { id =>
        DynamoDBQuery.getItem(table, PrimaryKey("id" -> id))
      }
      .withBatchUnprocessedRetryPolicy(RetryPolicy.ExponentialBackoff(maxRetries = 3, initialDelay = 50.millis))

  // Consistency is sent per table
  val batchGetStrong: DynamoDBQuery[Any, DynamoDBBatch.GetResult] =
    DynamoDBQuery.batchGetItem(orderIds) { id =>
      DynamoDBQuery.getItem(table, PrimaryKey("id" -> id)).consistency(ConsistencyMode.Strong)
    }

  val batchWrite: DynamoDBQuery[Any, DynamoDBBatch.WriteResult] =
    DynamoDBQuery.batchWriteItem(orderIds) { id =>
      DynamoDBQuery.putItem(table, Item("id" -> id, "status" -> "pending"))
    }

  val batchDelete: DynamoDBQuery[Any, DynamoDBBatch.WriteResult] =
    DynamoDBQuery.batchWriteItem(orderIds) { id =>
      DynamoDBQuery.deleteItem(table, PrimaryKey("id" -> id))
    }

  // Put and delete mixed in one batch, decided by matching on the input's own shape.
  sealed trait OrderSync
  object OrderSync {
    final case class Upsert(id: String) extends OrderSync
    final case class Remove(id: String) extends OrderSync
  }

  private val orderSyncs: List[OrderSync] = List(OrderSync.Upsert("o1"), OrderSync.Remove("o2"))

  val batchWriteMixed: DynamoDBQuery[Any, DynamoDBBatch.WriteResult] =
    DynamoDBQuery.batchWriteItem(orderSyncs) {
      case OrderSync.Upsert(id) => DynamoDBQuery.putItem(table, Item("id" -> id, "status" -> "pending"))
      case OrderSync.Remove(id) => DynamoDBQuery.deleteItem(table, PrimaryKey("id" -> id))
    }

  // Gets from two tables in one batch, the table chosen by matching on the input's own shape.
  private val customerTable = "customers"

  sealed trait Lookup
  object Lookup {
    final case class Order(id: String)    extends Lookup
    final case class Customer(id: String) extends Lookup
  }

  private val lookups: List[Lookup] = List(Lookup.Order("o1"), Lookup.Customer("c1"), Lookup.Order("o2"))

  val batchGetMixed: DynamoDBQuery[Any, DynamoDBBatch.GetResult] =
    DynamoDBQuery.batchGetItem(lookups) {
      case Lookup.Order(id)    => DynamoDBQuery.getItem(table, PrimaryKey("id" -> id))
      case Lookup.Customer(id) => DynamoDBQuery.getItem(customerTable, PrimaryKey("id" -> id))
    }

  // Items come back grouped by table name (`responses: Map[String, Chunk[Item]]`); a key with no
  // matching item is simply absent.
  def ordersAndCustomers(result: DynamoDBBatch.GetResult): (Chunk[Item], Chunk[Item]) =
    result match {
      case DynamoDBBatch.GetResult.Complete(response)   =>
        (response.responses.getOrElse(table, Chunk.empty), response.responses.getOrElse(customerTable, Chunk.empty))
      case DynamoDBBatch.GetResult.Incomplete(response) => // what was read; response.unprocessedKeys holds the rest
        (response.responses.getOrElse(table, Chunk.empty), response.responses.getOrElse(customerTable, Chunk.empty))
      case DynamoDBBatch.GetResult.Failed(_, _, _)      => (Chunk.empty, Chunk.empty)
    }
}
