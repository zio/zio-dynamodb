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

package zio.dynamodb

/**
 * Controls which attributes are included in the response of a `Query` or
 * `Scan` operation.  Maps to the `Select` parameter of the AWS DynamoDB
 * [[https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Select Query]]
 * and
 * [[https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-Select Scan]]
 * APIs.
 *
 * Applied via `.selectAllAttributes`, `.selectAllProjectedAttributes`,
 * `.selectSpecificAttributes`, or `.selectCount` on a `query` /
 * `scan` query.
 *
 * Constraint: if a projection expression is also present on the query, the
 * only valid value is `SpecificAttributes` — AWS rejects any other
 * combination with a `ValidationException`.
 */
sealed trait Select

object Select {

  /**
   * Returns all attributes of every matching item.
   *
   * When querying a local secondary index whose projection does not cover
   * all attributes, DynamoDB fetches each missing attribute from the parent
   * table.  This back-fill incurs additional read-capacity units and latency.
   *
   * AWS value: `ALL_ATTRIBUTES`.
   */
  case object AllAttributes extends Select

  /**
   * Returns only the attributes that were projected into the index at
   * creation time.
   *
   * Valid only when targeting a Global Secondary Index or Local Secondary
   * Index; DynamoDB rejects this value for base-table queries/scans.  If
   * the index was created with `ProjectionType.All`, the result is
   * equivalent to `AllAttributes`.
   *
   * AWS value: `ALL_PROJECTED_ATTRIBUTES`.
   */
  case object AllProjectedAttributes extends Select

  /**
   * Returns only the attributes listed in the query's projection expression.
   *
   * This is the only `Select` value permitted when a projection expression
   * is also set.  If the query has a projection expression and no explicit
   * `Select`, AWS treats the request as `SPECIFIC_ATTRIBUTES` implicitly.
   *
   * AWS value: `SPECIFIC_ATTRIBUTES`.
   */
  case object SpecificAttributes extends Select

  /**
   * Returns the count of matching items rather than the items themselves.
   * DynamoDB populates the `Count` field of the response and returns no
   * item data.
   *
   * When this value is set, the returned `Page` will have an empty `items`
   * chunk; the match count is available via `Page.count`.
   *
   * Read capacity consumed is the same as a normal query or scan.
   *
   * AWS value: `COUNT`.
   */
  case object Count extends Select
}
