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

import zio.dynamodb.{ Batch => DynamoDBBatch, DynamoDBQuery, Item, PrimaryKey }

/**
 * `batchGetItem`/`batchWriteItem` build one batch request from a `Iterable[A]` plus a function
 * from each `A` to a single-item `getItem`/`putItem`/`deleteItem` query — there's no per-model
 * derivation involved, `A` can be anything the caller has on hand (here, a plain order ID).
 */
object Batch {

  private val table = "orders"

  private val orderIds: List[String] = List("o1", "o2")

  val batchGet: DynamoDBQuery[Any, DynamoDBBatch.GetResult] =
    DynamoDBQuery.batchGetItem(orderIds) { id =>
      DynamoDBQuery.getItem(table, PrimaryKey("id" -> id))
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
}
