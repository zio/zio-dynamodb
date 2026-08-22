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

import zio.dynamodb.KeyConditionExpr.{ ExtendedSortKeyExpr, SortKeyEquals }
import zio.dynamodb.ProjectionExpression.Unknown

private[dynamodb] final case class SortKey[-From, +To](keyName: String)

/**
 * Adds comparison operators (`===`, `<`, `>`, `between`, `beginsWith`, ...) to a
 * `ProjectionExpression` marked as a sort key (via `.sortKey`), each building a
 * [[KeyConditionExpr]] case for use in `query`'s `.whereKey`. Applies to `String`,
 * `Number`, and `Binary`-encoded values — DynamoDB's own restriction on sort-key comparisons.
 */
object SortKey {
  // all comparison ops apply to: Strings, Numbers, Binary values

  implicit class SortKeyUnknownToOps[-From](val sk: SortKey[From, Unknown]) {
    def ===[To: ToAttributeValue](
      value: To
    ): SortKeyEquals[From]                                                             =
      SortKeyEquals(sk, ToAttributeValue[To].toAttributeValue(value))
    def >[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.GreaterThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def <[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.LessThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def <>[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.NotEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def <=[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.LessThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def >=[To: ToAttributeValue](
      value: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.GreaterThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def between[To: ToAttributeValue](min: To, max: To): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.Between[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(min),
        ToAttributeValue[To].toAttributeValue(max)
      )
    def beginsWith[To: ToAttributeValue](
      prefix: To
    ): ExtendedSortKeyExpr[From, To]                                                   =
      ExtendedSortKeyExpr.BeginsWith[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(prefix)
      )
  }

  implicit class SortKeyOps[-From, To: ToAttributeValue](val sk: SortKey[From, To]) {
    def ===(
      value: To
    ): SortKeyEquals[From]                                       =
      SortKeyEquals(sk, ToAttributeValue[To].toAttributeValue(value))
    def >(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.GreaterThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def <(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.LessThan(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def <>(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.NotEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def <=(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.LessThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def >=(
      value: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.GreaterThanOrEqual(
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(value)
      )
    def between(min: To, max: To): ExtendedSortKeyExpr[From, To] =
      ExtendedSortKeyExpr.Between[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(min),
        ToAttributeValue[To].toAttributeValue(max)
      )
    def beginsWith(
      prefix: To
    ): ExtendedSortKeyExpr[From, To]                             =
      ExtendedSortKeyExpr.BeginsWith[From, To](
        sk.asInstanceOf[SortKey[From, To]],
        ToAttributeValue[To].toAttributeValue(prefix)
      )

  }

}
