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

import zio.blocks.chunk.Chunk
import zio.dynamodb.{ AttributeValueType, ConditionExpression, DynamoDBQuery, Item, Page, PrimaryKey }
import zio.dynamodb.ProjectionExpression.$

/**
 * Relational and range operators (`===`, `<>`, `<`, `<=`, `>`, `>=`, `between`, `in`, `inSet`,
 * `attributeExists`, `attributeNotExists`, `attributeType`) against DynamoDB's scalar attribute
 * types: numbers, strings, booleans, and byte sequences (`Chunk[Byte]`).
 */
object Scalars {

  private val table = "widgets"

  // S — relational + range
  val sEq: ConditionExpression[Any]      = $("id") === "w-1"
  val sNeq: ConditionExpression[Any]     = $("id") <> "w-1"
  val sLt: ConditionExpression[Any]      = $("id") < "w-9"
  val sBetween: ConditionExpression[Any] = $("id").between("w-1", "w-9")
  val sIn: ConditionExpression[Any]      = $("id").in("w-1", "w-2", "w-3")
  val sInSet: ConditionExpression[Any]   = $("id").inSet(Set("w-1", "w-2", "w-3")) // same as .in, Set literal

  // N — relational + range
  val nEq: ConditionExpression[Any]      = $("qty") === 10
  val nGte: ConditionExpression[Any]     = $("qty") >= 1
  val nLte: ConditionExpression[Any]     = $("price") <= 99.99
  val nBetween: ConditionExpression[Any] = $("qty").between(1, 100)
  val nIn: ConditionExpression[Any]      = $("qty").in(1, 2, 3)

  // BOOL — equality only; range/between/in are meaningless for a two-valued type and the
  // library doesn't expose them for it
  val boolEq: ConditionExpression[Any] = $("active") === true

  // B — relational + range (byte sequences compare lexicographically on the wire)
  val bEq: ConditionExpression[Any]      = $("blob") === Chunk[Byte](1, 2, 3)
  val bBetween: ConditionExpression[Any] = $("blob").between(Chunk[Byte](0), Chunk[Byte](-1))

  // Attribute presence and stored type
  val exists: ConditionExpression[Any]    = $("id").attributeExists
  val notExists: ConditionExpression[Any] = $("discontinuedAt").attributeNotExists
  val isNumber: ConditionExpression[Any]  = $("qty").attributeType(AttributeValueType.Number)

  // Wired into the six CRUD operations — see ScalarsSpec for these actually run.
  val item: Item      = Item("id" -> "w-1", "qty" -> 10, "price" -> 9.99, "active" -> true, "blob" -> Chunk[Byte](1, 2, 3))
  val key: PrimaryKey = PrimaryKey("id" -> "w-1")

  val putQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.putItem(table, item)

  val getQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.getItem(table, key)

  val updateQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.updateItem(table, key)($("qty").set(5))

  val deleteQuery: DynamoDBQuery[Any, Option[Item]] =
    DynamoDBQuery.deleteItem(table, key)

  val queryQuery: DynamoDBQuery[Any, Page[Item]] =
    DynamoDBQuery.query(table, limit = 20).whereKey($("id").partitionKey === "w-1").filter(nGte)

  val scanQuery: DynamoDBQuery[Any, Page[Item]] =
    DynamoDBQuery.scan(table, limit = 20).filter(sBetween && nLte)
}
