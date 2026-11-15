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

package examples.highlevelapi

import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, Schema }
import zio.dynamodb.{ Batch, DynamoDBQuery }
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver
import zio.dynamodb.blocks.schema.ddbexpr.dsl._

/**
 * High-Level `put` / `deleteFrom` as the items of a `batchWriteItem`. Each write goes through the
 * table's codec - so the renamed key field below (`customer_id`) is used for both puts and
 * deletes, where a hand-written `PrimaryKey("id" -> ...)` would silently delete nothing. The batch
 * itself stays Low-Level: it executes to a `Batch.WriteResult`, and what to do with unprocessed
 * items is your call. Only plain puts and deletes compile here - no `.where`, `.returning` or update.
 */
object BatchWrites {

  final case class Customer(id: String, name: String)

  object Customer extends CompanionOptics[Customer] {
    implicit val schema: Schema[Customer] = Schema.derived

    val id: Lens[Customer, String]   = $(_.id)
    val name: Lens[Customer, String] = $(_.name)
  }

  val customerTable: Table[Customer] =
    Table(
      "customers",
      Customer.schema.deriving(DynamoDBCodecDeriver).modifier(Customer.id, Modifier.rename("customer_id")).derive
    )

  val customers: List[Customer] = List(Customer("c1", "Ada"), Customer("c2", "Grace"))

  val putAll: DynamoDBQuery[Any, Batch.WriteResult] =
    DynamoDBQuery.batchWriteItem(customers)(c => put(customerTable, c)) // max 25 per batch

  val deleteAll: DynamoDBQuery[Any, Batch.WriteResult] =
    DynamoDBQuery.batchWriteItem(List("c1", "c2"))(id => deleteFrom(customerTable)(Customer.id.partitionKey === id))
}
