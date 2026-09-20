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

/**
 * `.where` (put/update/delete) and `.filter` (scan/query), combined with `&&`/`||`/`!`.
 * `naivelyMisusedFilter` shows a case the compile-time check misses — see
 * `CompileTimeRejectionsSpec.scala` for how pinning the condition's type restores it.
 */
object ConditionsAndFilters {

  private val table = "accounts"
  private val key   = PrimaryKey("id" -> "a1")

  val isActive: ConditionExpression[Any]   = $("active") === true
  val hasBalance: ConditionExpression[Any] = $("balance") > 0
  val combined: ConditionExpression[Any]   = isActive && hasBalance
  val either: ConditionExpression[Any]     = isActive || hasBalance
  val negated: ConditionExpression[Any]    = !isActive

  // Intended usage: .where on a write operation, with a combined (&&) condition.
  val putWithCondition: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, Item("id" -> "a1", "active" -> true, "balance" -> 100)).where(combined)

  // Intended usage: .filter on a read operation, with an either (||) condition.
  val scanWithFilter = DynamoDBQuery.scan(table, limit = 20).filter(either)

  // .where with a negated (!) condition.
  val deleteWithNegatedCondition: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.deleteItem(table, key).where(negated)

  // Compiles despite being the wrong operation for .filter — caught at run time instead.
  val naivelyMisusedFilter: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, Item("id" -> "a1", "active" -> true, "balance" -> 100)).filter(isActive)
}
