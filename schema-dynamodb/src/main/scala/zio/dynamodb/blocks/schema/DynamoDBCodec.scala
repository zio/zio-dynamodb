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

package zio.dynamodb.blocks.schema

import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.{ Decoder, Encoder, FromAttributeValue, Item, ToAttributeValue }

/**
 * Encodes/decodes between `A` and [[zio.dynamodb.AttributeValue]] — produced by
 * `Schema[A].deriving(DynamoDBCodecDeriver).derive` for a model with a zio-blocks `Schema`,
 * or written by hand for a type that can't derive one. `toItem`/`fromItem` are the bridge to
 * the Low-Level API's `Item`-shaped operations; the High-Level API (`DdbExprApi`/`dsl`)
 * derives and caches one of these per model type automatically.
 */
abstract class DynamoDBCodec[A](val valueType: Int = DynamoDBCodec.objectType) {

  def encoder: Encoder[A]
  def decoder: Decoder[A]

  val recordFieldNames: Chunk[String] = Chunk.empty

  /**
   * Encodes `a` directly to an [[Item]] — the bridge between this codec and the LL API's
   *  `DynamoDBQuery.putItem`/`getItem`/etc., which speak `Item`, not `AttributeValue`
   *  directly. Only meaningful for a record-shaped codec (`A` encoding to a top-level
   *  map, i.e. a model used as a whole table item) — throws for a scalar/primitive codec,
   *  which was never going to be a valid whole item anyway.
   */
  def toItem(a: A): Item =
    FromAttributeValue[Item]
      .fromAttributeValue(encoder(a))
      .fold(
        err => throw new IllegalArgumentException(s"codec did not encode to a record/map: ${err.message}"),
        identity
      )

  /**
   * The other direction of [[toItem]]: decodes an [[Item]] (as returned by
   *  `DynamoDBQuery.getItem`/`scanSome`/`querySome`) back into `A`.
   */
  def fromItem(item: Item): Either[ItemError.DecodingError, A] =
    decoder(ToAttributeValue[Item].toAttributeValue(item))

}

/** `valueType` tag constants used internally to dispatch on a field's primitive register type without boxing. */
object DynamoDBCodec {
  val objectType  = 0
  val intType     = 1
  val booleanType = 2
  val longType    = 3
  val byteType    = 4
  val charType    = 5
  val shortType   = 6
  val floatType   = 7
  val doubleType  = 8
  val unitType    = 9
}
