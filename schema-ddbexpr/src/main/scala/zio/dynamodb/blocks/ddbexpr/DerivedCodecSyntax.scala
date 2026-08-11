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

package zio.dynamodb.blocks.ddbexpr

import zio.blocks.schema.Schema
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

// Shared by DdbExprSyntax and DdbKeyExprSyntax so both can resolve DynamoDBCodec[A]
// implicitly without each declaring their own copy of derivedCodec. Trait linearization
// dedupes a trait mixed in via multiple paths, so a facade extending both no longer hits
// an ambiguous-implicit error the way importing DdbExpr._ and DdbKeyExpr._ together used to.
trait DerivedCodecSyntax {
  implicit def derivedCodec[A](implicit
    schema: Schema[A],
    cfg: DynamoDBCodecDeriverConfigure[A]
  ): DynamoDBCodec[A] =
    schema.deriving(cfg.configure(DynamoDBCodecDeriver)).derive
}
