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

package zio.dynamodb.blocks

import zio.blocks.schema.derive.Deriver
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

trait DynamoDBCodecDeriverConfigure[+A] {
  // Returns Deriver[DynamoDBCodec] rather than DynamoDBCodecDeriver so that
  // callers can chain withModifier / withInstance on the base Deriver API,
  // which returns Deriver[DynamoDBCodec] (not a DynamoDBCodecDeriver subtype).
  def configure(d: DynamoDBCodecDeriver): Deriver[DynamoDBCodec]
}

object DynamoDBCodecDeriverConfigure {
  // Singleton so default[A] always returns the same reference — required for
  // identity-based cache keys in DdbSchemaExprApi.  Covariance (+A) makes widening safe.
  private val _identity: DynamoDBCodecDeriverConfigure[Nothing] = d => d

  def identity[A]: DynamoDBCodecDeriverConfigure[A]         = _identity
  implicit def default[A]: DynamoDBCodecDeriverConfigure[A] = identity
}
