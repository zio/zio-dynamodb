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
 * Paths chain freely across maps and lists — `$("orders")(0)("lineItems")(2)("sku")` reaches
 * three levels deep without needing a type describing the shape in between, since a
 * `ProjectionExpression` is just a sequence of map-key/list-index segments rendered as a
 * dotted/bracketed string (`orders[0].lineItems[2].sku`).
 */
object NestedPaths {

  private val table = "carts"
  private val key   = PrimaryKey("id" -> "c1")

  val firstOrderFirstLineItemSku: ConditionExpression[Any] =
    $("orders")(0)("lineItems")(0)("sku") === "widget"

  val setQuantity: Action[Any] =
    $("orders")(0)("lineItems")(0)("qty").set(3)

  val item: Item = Item(
    "id"     -> "c1",
    "orders" -> List(Item("lineItems" -> List(Item("sku" -> "widget", "qty" -> 1))))
  )

  val putQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, item)

  val updateQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)(setQuantity)

  val scanQuery = DynamoDBQuery.scan(table, limit = 20).filter(firstOrderFirstLineItemSku)
}
