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

import zio.dynamodb.{ DynamoDBQuery, Item, Page, PrimaryKey }
import zio.dynamodb.ProjectionExpression.$

/**
 * The six CRUD operations — `getItem`, `putItem`, `updateItem`, `deleteItem`, `query`, `scan`
 * — called directly on `DynamoDBQuery`'s companion object, with no builder layer in between.
 * `.where`/`.filter`/`.whereKey` attach straight onto the returned `DynamoDBQuery`, since
 * they're methods on `DynamoDBQuery` itself rather than on a separate builder type.
 */
object CrudBuilders {

  private val table = "tasks"
  private val key   = PrimaryKey("id" -> "t1")

  val putQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, Item("id" -> "t1", "title" -> "write docs", "done" -> false, "priority" -> 1))

  val putWithCondition: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery
      .putItem(table, Item("id" -> "t1", "title" -> "write docs", "done" -> false, "priority" -> 1))
      .where($("id").attributeNotExists)

  val getQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.getItem(table, key)

  val deleteQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.deleteItem(table, key)

  val deleteWithCondition: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.deleteItem(table, key).where($("done") === true)

  val updateQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)($("done").set(true))

  val updateWithCondition: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)($("priority").increment(1)).where($("done") === false)

  val queryQuery: DynamoDBQuery[Any, Page[Item]] =
    DynamoDBQuery
      .query(table, limit = 20)
      .whereKey($("id").partitionKey === "t1")
      .filter($("priority") > 0)
      .sortOrder(ascending = false)

  val scanQuery: DynamoDBQuery[Any, Page[Item]] =
    DynamoDBQuery.scan(table, limit = 20).filter($("done") === false)
}
