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

import java.util.concurrent.ConcurrentHashMap
import zio.blocks.schema.{ DynamicOptic, Optic, Schema }
import zio.dynamodb.{ AttributeValue, ProjectionExpression }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.{ OpticToPE, ProjectionResolver }
import zio.dynamodb.blocks.schema.DynamoDBCodec

/**
 * Per-[[Table]] expression-resolution context. `config` and `resolver` are invariant for a
 *  given table, so the object that threads them - and the literal-codec lookups it keys -
 *  is built once here rather than allocated on every `.where` / `.filter` / key-condition
 *  construction. Attribute-name resolution itself is delegated to [[ProjectionResolver]] - a
 *  deriver-produced [[zio.dynamodb.blocks.schema.Resolver]] tree, not a schema-plus-config
 *  re-derivation on every call.
 *
 *  `resolver == null` marks the config-free path the low-level implicit conversions use
 *  (`.filter` / `.whereKey` on a bare [[zio.dynamodb.DynamoDBQuery]]): raw optic names, no
 *  derivation at all. That path shares the one [[ExprCtx.default]] instance.
 */
private[ddbexpr] final class ExprCtx(
  private[ddbexpr] val config: DynamoDBCodecDeriverConfigure[_],
  private[ddbexpr] val resolver: ProjectionResolver[_]
) {

  // Keyed by CodecCacheKey (identity hash of the Schema + the config's cached hashCode).
  // Avoids a structural `Schema#hashCode`, which walks the literal's Reflect.
  private[this] val codecCache = new ConcurrentHashMap[CodecCacheKey, DynamoDBCodec[_]]()

  private[ddbexpr] def peOf(optic: Optic[_, _]): Either[String, ProjectionExpression[_, _]] =
    if (resolver eq null) OpticToPE.pe(optic) else resolver.resolve(optic.toDynamic)

  private[ddbexpr] def peOf(dyn: DynamicOptic): Either[String, ProjectionExpression[_, _]] =
    if (resolver eq null) OpticToPE.pe(dyn) else resolver.resolve(dyn)

  private[ddbexpr] def codecOf[A](schema: Schema[A]): DynamoDBCodec[A] = {
    val key = new CodecCacheKey(schema, config)
    val hit = codecCache.get(key)
    if (hit ne null) hit.asInstanceOf[DynamoDBCodec[A]]
    else {
      val derived = schema.deriving(config.toDeriver).derive
      codecCache.putIfAbsent(key, derived)
      derived.asInstanceOf[DynamoDBCodec[A]]
    }
  }

  private[ddbexpr] def encode[A](value: A, schema: Schema[A]): AttributeValue =
    codecOf(schema).encoder(value)
}

private[ddbexpr] object ExprCtx {
  val default: ExprCtx = new ExprCtx(DynamoDBCodecDeriverConfigure.default[Any], null)
}
