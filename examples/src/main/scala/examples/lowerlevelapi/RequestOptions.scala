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

import zio.dynamodb.{ ConsistencyMode, DynamoDBQuery, Item, PrimaryKey, ReturnConsumedCapacity, ReturnValues }
import zio.dynamodb.ProjectionExpression.$

/**
 * Per-request configuration knobs, all defined directly on `DynamoDBQuery` rather than on a
 * separate builder type: `.capacity` (`ReturnConsumedCapacity`), `.returns` (`ReturnValues`),
 * `.consistency` (`ConsistencyMode`), `.sortOrder`, `.startKey` (pagination), `.limit`. The
 * Low-Level API has no schema-derivation configuration to speak of — there's no schema — so
 * this file's counterpart to the High-Level API's `TableConfigs.scala` is these request-level
 * knobs instead.
 */
object RequestOptions {

  private val table = "orders"
  private val key   = PrimaryKey("id" -> "o1")

  val putWithCapacity: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, Item("id" -> "o1")).capacity(ReturnConsumedCapacity.Total)

  val updateWithReturnValues: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)($("status").set("shipped")).returns(ReturnValues.AllNew)

  val strictlyConsistentGet: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.getItem(table, key).consistency(ConsistencyMode.Strong)

  val descendingQuery =
    DynamoDBQuery.query(table, limit = 20).whereKey($("id").partitionKey === "o1").sortOrder(ascending = false)

  val nextPage = DynamoDBQuery.scan(table, limit = 20).startKey(Some(key))
}
