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

import zio.dynamodb.KeyConditionExpr.PartitionKeyEquals
import zio.dynamodb.ProjectionExpression.Unknown

private[dynamodb] final case class PartitionKey[-From, +To](keyName: String)
object PartitionKey {
  implicit class PartitionKeyUnknownToOps[-From](val pk: PartitionKey[From, Unknown])         {
    def ===[To: ToAttributeValue](
      value: To
    ): PartitionKeyEquals[From] =
      PartitionKeyEquals(pk, ToAttributeValue[To].toAttributeValue(value))
  }
  implicit class PartitionKeyOps[-From, To: ToAttributeValue](val pk: PartitionKey[From, To]) {
    def ===(
      value: To
    ): PartitionKeyEquals[From] =
      PartitionKeyEquals(pk, ToAttributeValue[To].toAttributeValue(value))
  }

}
