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

import zio.blocks.chunk.Chunk
import zio.dynamodb.ProjectionExpression.{ $, Unknown }

object Client {
  final case class Person(id: String, name: String)
  val item: AttrMap                             = Item("id" -> "123", "name" -> "test")
  val putItem: DynamoDBQuery[Any, Option[Item]] = DynamoDBQuery.putItem("my-table", item)
  val putItemResult: DummyIO[Option[Item]]      = DummyIOInterpreter.run(putItem)

  val people: List[Person]                              = List(Person("123", "test"))
  val batchWriteItems: DynamoDBQuery.BatchWriteItem     =
    DynamoDBQuery.batchWriteItem(people)(person =>
      DynamoDBQuery.putItem("my-table", Item("id" -> person.id, "name" -> person.name))
    )
  val batchWriteItemsResult: DummyIO[Batch.WriteResult] = DummyIOInterpreter.run(batchWriteItems)

  val batchGetItem: DynamoDBQuery.BatchGetItem =
    DynamoDBQuery.batchGetItem(people) { person =>
      DynamoDBQuery.getItem("my-table", PrimaryKey("id" -> person.id))
    }

  val getItem: DynamoDBQuery[Any, Option[Item]] = DynamoDBQuery.getItem("my-table", PrimaryKey("id" -> "123"))
  val getItemResult: DummyIO[Option[Item]]      = DummyIOInterpreter.run(getItem)

  val updateItem: DynamoDBQuery[Any, Option[Item]] = DynamoDBQuery.updateItem("my-table", PrimaryKey("id" -> "123")) {
    $("count").set(1)
  }

  val xs: Chunk[DynamoDBQuery[Any, Option[Item]]] = Chunk(updateItem)

  val peName: ProjectionExpression[Any, Unknown] = $("name")

  // Condition/Filter expressions
  val expr: FilterExpression[Any]                                  = $("name") === "test" && $("age") > 18
  // KeyCondition expressions - primary keys
  val expr1: KeyConditionExpr.PartitionKeyEquals[Any]              = $("name").partitionKey === "test"
  val expr2: KeyConditionExpr.CompositePrimaryKeyExpr[Any]         =
    $("name").partitionKey === "test" && $("age").sortKey === 18
  // KeyCondition expressions - not a primary key but a key condition expression that can be used in Query operations
  val expr3: KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[Any] =
    $("name").partitionKey === "test" && $("age").sortKey > 18

  /*
  item.zip(item)
  PROBLEMS WITH AUTO BATCHING
  - too powerful - interface presented to the user is so abstract that users get confused when things go wrong
  - not needed - users have clear intent on policies around parallelism and batching before they write code

  PROBLEMS WITH BATCHING:
  - batched API has rules - users are guaranteed to make mistakes with a generic batch API
  RULES
  2 API's
  - Batched reads - only Get
  - Batched writes - only Put and Delete

  WHEN DOES A USER NEED PARALLEL EXECUTION?
  - when they cant batch eg a bunch or Updates

   */
}
