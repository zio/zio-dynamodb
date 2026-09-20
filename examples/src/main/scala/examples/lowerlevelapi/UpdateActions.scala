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

import zio.dynamodb.{ DynamoDBQuery, Item, PrimaryKey }
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.{ Action, RenderableAction }

/**
 * `+` combines actions of different kinds (SET/REMOVE/ADD/DELETE) into one `UpdateItem` call.
 */
object UpdateActions {

  private val table = "accounts"
  private val key   = PrimaryKey("id" -> "a1")

  val setBalance: RenderableAction[Any]       = $("balance").set(100)
  val setIfAbsent: RenderableAction[Any]      = $("tier").setIfNotExists("standard")
  val incrementBalance: RenderableAction[Any] = $("balance").increment(50)
  val decrementBalance: RenderableAction[Any] = $("balance").decrement(20)
  val addToCounter: RenderableAction[Any]     = $("loginCount").add(1)
  val removeFlag: RenderableAction[Any]       = $("suspended").remove
  val combined: Action[Any]                   = incrementBalance + removeFlag + addToCounter

  val updateQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)(combined)
}
