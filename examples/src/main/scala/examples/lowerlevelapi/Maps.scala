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

import zio.dynamodb.{ ConditionExpression, DynamoDBQuery, Item, PrimaryKey }
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.Action

/**
 * Map attributes: `apply(key: String)` addresses a nested attribute by string key, for reads,
 * writes, and conditions alike.
 */
object Maps {

  private val table = "profiles"
  private val key   = PrimaryKey("id" -> "u1")

  val cityExists: ConditionExpression[Any] = $("address")("city").attributeExists

  val setCity: Action[Any]   = $("address")("city").set("London")
  val removeZip: Action[Any] = $("address")("zip").remove

  val item: Item = Item("id" -> "u1", "address" -> Item("city" -> "London", "zip" -> "SW1A 1AA"))

  val putQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, item)

  val updateQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)(setCity)

  val scanQuery = DynamoDBQuery.scan(table, limit = 20).filter(cityExists)
}
