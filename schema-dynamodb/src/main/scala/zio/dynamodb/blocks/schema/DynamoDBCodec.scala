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
import zio.blocks.schema.{ DynamicOptic, DynamicValue, PrimitiveValue }
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.{ Decoder, Encoder, FromAttributeValue, Item, ToAttributeValue }

/**
 * Encodes/decodes between `A` and [[zio.dynamodb.AttributeValue]] — produced by
 * `Schema[A].deriving(DynamoDBCodecDeriver).derive` for a model with a zio-blocks `Schema`,
 * or written by hand for a type that can't derive one. `toItem`/`fromItem` are the bridge to
 * the Low-Level API's `Item`-shaped operations; the High-Level API (`DdbExprApi`/`dsl`)
 * derives and caches one of these per model type automatically.
 */
abstract class DynamoDBCodec[A] {

  private[dynamodb] val valueType: Int = DynamoDBCodec.objectType

  def encoder: Encoder[A]
  def decoder: Decoder[A]

  /**
   * One optic step into the value this codec encodes: the attribute-path segment the step adds and
   * the codec at that position. Lets the library resolve an optic against the codec that actually
   * writes the item, rather than restating its naming rules. A codec that doesn't override it (for
   * example a hand-written one) can't be navigated.
   */
  private[dynamodb] def step(node: DynamicOptic.Node): Either[String, DynamoDBCodec.Step] =
    DynamoDBCodec.notNavigable(node)

  private[dynamodb] val recordFieldNames: Chunk[String] = Chunk.empty

  /**
   * Scala field name -> DynamoDB attribute name, for a record-shaped codec. Lets callers
   * resolve an optic's raw field name to the wire name this codec's configuration produced
   * (field-name mapper, `@Modifier.rename`, …). Empty for non-record codecs.
   */
  private[dynamodb] val recordFieldNameMap: Map[String, String] = Map.empty

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
   *  `DynamoDBQuery.getItem`/`scan`/`query`) back into `A`.
   */
  def fromItem(item: Item): Either[ItemError.DecodingError, A] =
    decoder(ToAttributeValue[Item].toAttributeValue(item))

}

/** `valueType` tag constants used internally to dispatch on a field's primitive register type without boxing. */
object DynamoDBCodec {
  private[dynamodb] val objectType  = 0
  private[dynamodb] val intType     = 1
  private[dynamodb] val booleanType = 2
  private[dynamodb] val longType    = 3
  private[dynamodb] val byteType    = 4
  private[dynamodb] val charType    = 5
  private[dynamodb] val shortType   = 6
  private[dynamodb] val floatType   = 7
  private[dynamodb] val doubleType  = 8
  private[dynamodb] val unitType    = 9

  private[dynamodb] final class Step(val segment: Segment, val codec: DynamoDBCodec[Any])

  private[dynamodb] sealed trait Segment
  private[dynamodb] object Segment {
    case object Transparent             extends Segment
    final case class Name(name: String) extends Segment
    final case class Index(index: Int)  extends Segment
  }

  private[dynamodb] def stepTo(segment: Segment, codec: DynamoDBCodec[_]): Either[String, Step] =
    Right(new Step(segment, codec.asInstanceOf[DynamoDBCodec[Any]]))

  private[dynamodb] def notNavigable(node: DynamicOptic.Node): Either[String, Step] =
    node match {
      case DynamicOptic.Node.Field(name)                                                  =>
        Left(s"path segment '$name' is not a record field")
      case DynamicOptic.Node.Case(name)                                                   =>
        Left(s"path segment 'case $name' is not a variant")
      case DynamicOptic.Node.AtIndex(index)                                               =>
        Left(s"'$index' is not a sequence index")
      case DynamicOptic.Node.AtMapKey(DynamicValue.Primitive(PrimitiveValue.String(key))) =>
        Left(s"'$key' is not a map key")
      case DynamicOptic.Node.AtMapKey(key)                                                =>
        Left(s"found map key '$key' — only String keys are supported in DDB")
      case other                                                                          =>
        Left(s"unexpected optic node: $other")
    }
}
