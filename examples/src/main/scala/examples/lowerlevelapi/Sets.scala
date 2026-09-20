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
import zio.dynamodb.UpdateExpression.RenderableAction

/**
 * DynamoDB's native set types — `StringSet`, `NumberSet`, `BinarySet` — via `contains`,
 * `containsSet` (conditions) and `addSet`, `deleteFromSet` (update actions). `inSet` (see
 * `Scalars.scala`) is a different thing — an `IN` condition testing a scalar attribute against
 * a Scala `Set` of candidates — and isn't valid here against a Set-typed attribute.
 */
object Sets {

  private val table = "products"
  private val key   = PrimaryKey("id" -> "p1")

  // Conditions
  val hasTag: ConditionExpression[Any]         = $("tags").contains("clearance")
  val hasAllTags: ConditionExpression[Any]     = $("tags").containsSet("clearance", Set("new", "featured"))
  val ratingContains: ConditionExpression[Any] = $("ratings").contains(4)

  // Update actions
  val addTags: RenderableAction[Any]    = $("tags").addSet(Set("clearance", "sale"))
  val removeTags: RenderableAction[Any] = $("tags").deleteFromSet(Set("sale"))
  val addRatings: RenderableAction[Any] = $("ratings").addSet(Set(4, 5))

  val item: Item = Item("id" -> "p1", "tags" -> Set("new", "featured"), "ratings" -> Set(4, 5))

  val putQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, item)

  val updateQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)(addTags + addRatings)

  val scanQuery = DynamoDBQuery.scan(table, limit = 20).filter(hasTag)
}
