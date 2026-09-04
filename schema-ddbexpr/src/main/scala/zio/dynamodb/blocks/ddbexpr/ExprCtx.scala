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
import zio.blocks.schema.{ DynamicOptic, Optic, Reflect, Schema }
import zio.blocks.schema.binding.Binding
import zio.dynamodb.{ AttributeValue, ProjectionExpression }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.OpticToPE
import zio.dynamodb.blocks.schema.DynamoDBCodec

/**
 * Per-[[Table]] expression-resolution context. `config`, `rootReflect` and
 *  `recordFieldNameMap` are invariant for a given table, so the object that threads them
 *  and the lookups they key - resolved [[ProjectionExpression]]s and literal codecs - are
 *  built once here rather than allocated on every `.where` / `.filter` / key-condition
 *  construction.
 *
 *  `rootReflect == null` marks the config-free path the low-level implicit conversions use
 *  (`.filter` / `.whereKey` on a bare [[zio.dynamodb.DynamoDBQuery]]): raw optic names,
 *  default deriver. That path shares the one [[ExprCtx.default]] instance.
 */
private[ddbexpr] final class ExprCtx(
  private[ddbexpr] val config: DynamoDBCodecDeriverConfigure[_],
  private[ddbexpr] val rootReflect: Reflect[Binding, _],
  private[ddbexpr] val recordFieldNameMap: Map[String, String]
) {

  // Keyed by DynamicOptic: its hashCode is a combine of a few (cached) String hashes over a
  // short node list - cheap and allocation-free on a hit. Deliberately NOT keyed by the
  // Optic itself: `LensImpl#hashCode` hashes its source `Reflect.Record`s structurally,
  // which walks the whole (possibly deep) record schema on every lookup.
  private[this] val peCache = new ConcurrentHashMap[DynamicOptic, Either[String, ProjectionExpression[_, _]]]()

  // Keyed by CodecCacheKey (identity hash of the Schema + the config's cached hashCode).
  // Again avoids a structural `Schema#hashCode`, which walks the literal's Reflect.
  private[this] val codecCache = new ConcurrentHashMap[CodecCacheKey, DynamoDBCodec[_]]()

  // Optic path: resolve via OpticToPE's Optic overload (dispatches Lens vs Optional - the
  // latter prunes `Some`/`value` and handles index / map-key nodes), but key by the cheap
  // DynamicOptic. `Lens#toDynamic` is a cached `lazy val`, so a reused CompanionOptics
  // accessor `val` (the idiom) recomputes nothing.
  private[ddbexpr] def peOf(optic: Optic[_, _]): Either[String, ProjectionExpression[_, _]] = {
    val key = optic.toDynamic
    val hit = peCache.get(key)
    if (hit ne null) hit
    else {
      val computed = rawOpticPe(optic)
      peCache.putIfAbsent(key, computed)
      computed
    }
  }

  private[ddbexpr] def peOf(dyn: DynamicOptic): Either[String, ProjectionExpression[_, _]] = {
    val hit = peCache.get(dyn)
    if (hit ne null) hit
    else {
      val computed = rawDynPe(dyn)
      peCache.putIfAbsent(dyn, computed)
      computed
    }
  }

  private[this] def rawOpticPe(optic: Optic[_, _]): Either[String, ProjectionExpression[_, _]] =
    if (rootReflect == null) OpticToPE.pe(optic)
    else
      OpticToPE.pe(
        optic.asInstanceOf[Optic[Any, Any]],
        rootReflect.asInstanceOf[Reflect[Binding, Any]],
        config
      )

  private[this] def rawDynPe(dyn: DynamicOptic): Either[String, ProjectionExpression[_, _]] =
    if (rootReflect == null) OpticToPE.pe(dyn)
    else OpticToPE.pe(dyn, rootReflect, config)

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
  val default: ExprCtx =
    new ExprCtx(DynamoDBCodecDeriverConfigure.default[Any], null, Map.empty)
}
