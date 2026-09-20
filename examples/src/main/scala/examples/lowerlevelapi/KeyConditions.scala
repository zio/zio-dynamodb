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

import zio.dynamodb.{ DynamoDBQuery, Item, KeyConditionExpr }
import zio.dynamodb.ProjectionExpression.$

/**
 * `.partitionKey`/`.sortKey` mark a raw path for use in `query`'s `.whereKey`: partition-only
 * equality, a composite equality on both keys, or an extended composite with a sort-key range
 * (`between`/`beginsWith`/`>`/`<`/etc). `nestedPathAsKey` shows what happens when the path isn't
 * actually a top-level attribute.
 */
object KeyConditions {

  private val table = "invoices"

  val partitionOnly: KeyConditionExpr[Any] =
    $("customerId").partitionKey === "cust-1"

  val composite: KeyConditionExpr[Any] =
    $("customerId").partitionKey === "cust-1" && $("invoiceId").sortKey === "inv-1"

  val extendedRange: KeyConditionExpr[Any] =
    $("customerId").partitionKey === "cust-1" && $("invoiceId").sortKey > "inv-100"

  val extendedBetween: KeyConditionExpr[Any] =
    $("customerId").partitionKey === "cust-1" && $("invoiceId").sortKey.between("inv-100", "inv-200")

  // Compiles fine, throws IllegalArgumentException once evaluated — a def, not a val, so it
  // isn't evaluated just by loading this object; see KeyConditionsSpec's "live failure" test.
  def nestedPathAsKey = $("lineItems")(0).partitionKey

  val queryQuery: DynamoDBQuery[Any, zio.dynamodb.Page[Item]] =
    DynamoDBQuery.query(table, limit = 20).whereKey(extendedRange)
}
