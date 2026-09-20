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
 * List attributes: `append`/`appendList` and `prepend`/`prependList` grow a list without
 * reading it back first; `apply(index)` addresses an element by position for a condition or
 * for `remove`. Unlike a Set, a List attribute doesn't need every element to share a single
 * DynamoDB type.
 */
object Lists {

  private val table = "orders"
  private val key   = PrimaryKey("id" -> "o1")

  // Indexed element access
  val firstItem: ConditionExpression[Any] = $("items")(0) === "widget"

  // Update actions
  val appendOne: Action[Any]   = $("items").append("gadget")
  val appendMany: Action[Any]  = $("items").appendList(List("gadget", "gizmo"))
  val prependOne: Action[Any]  = $("items").prepend("priority-item")
  val removeFirst: Action[Any] = $("items")(0).remove

  val item: Item = Item("id" -> "o1", "items" -> List("widget", "gadget"))

  val putQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, item)

  val appendQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)(appendOne)

  val removeQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)(removeFirst)
}
