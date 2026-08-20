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

import zio.blocks.chunk.Chunk

import scala.collection.immutable.{ Map, Set }

/**
 * Converts a Scala value into the wire-level [[AttributeValue]] the Low-Level API's
 * `Item(...)` constructor and comparison operators (`$("field") === value`) encode with.
 * Instances for every primitive, `Option`, `Iterable`, `Map[String, _]`, and native
 * DynamoDB set type are provided implicitly by the companion object; write a custom
 * instance to support an application-specific type directly (rather than converting to a
 * supported type first).
 */
trait ToAttributeValue[A] {
  def toAttributeValue(a: A): AttributeValue
}

/** Built-in [[ToAttributeValue]] instances for primitives, collections, and `Option`. */
object ToAttributeValue extends ToAttributeValueLowPriorityImplicits0 {
  import Predef.{ String => ScalaString }

  def apply[A](implicit to: ToAttributeValue[A]): ToAttributeValue[A] = to

  implicit val attributeValueToAttributeValue: ToAttributeValue[AttributeValue] = av => av

  implicit def optionToAttributeValue[A](implicit ev: ToAttributeValue[A]): ToAttributeValue[Option[A]] = {
    case None    => AttributeValue.Null
    case Some(a) => ev.toAttributeValue(a)
  }

  implicit def binaryToAttributeValue[Col[A] <: Iterable[A], A <: Byte]: ToAttributeValue[Col[A]] =
    x => AttributeValue.Binary(x)

  implicit def byteToAttributeValue: ToAttributeValue[Byte] =
    a => AttributeValue.Binary(Chunk(a))

  implicit def binarySetToAttributeValue[Col1[A] <: Iterable[A], Col2[B] <: Iterable[B], B <: Byte]
    : ToAttributeValue[Col1[Col2[B]]]                          = AttributeValue.BinarySet(_)
  implicit val boolToAttributeValue: ToAttributeValue[Boolean] = AttributeValue.Bool(_)

  implicit val attrMapToAttributeValue: ToAttributeValue[AttrMap] =
    (attrMap: AttrMap) =>
      AttributeValue.Map {
        attrMap.map.map { case (key, value) =>
          (AttributeValue.String(key), value)
        }
      }

  implicit def mapToAttributeValue[A](implicit ev: ToAttributeValue[A]): ToAttributeValue[Map[ScalaString, A]] =
    (map: Map[ScalaString, A]) =>
      AttributeValue.Map(map.map { case (k, v) => (AttributeValue.String(k), ev.toAttributeValue(v)) })

  implicit val stringToAttributeValue: ToAttributeValue[ScalaString]            = AttributeValue.String(_)
  implicit val stringSetToAttributeValue: ToAttributeValue[Set[ScalaString]]    =
    AttributeValue.StringSet(_)
  // BigDecimal support
  implicit val bigDecimalToAttributeValue: ToAttributeValue[BigDecimal]         = AttributeValue.Number(_)
  implicit val bigDecimalSetToAttributeValue: ToAttributeValue[Set[BigDecimal]] = AttributeValue.NumberSet(_)
  // short support
  implicit val shortToAttributeValue: ToAttributeValue[Short]                   = (a: Short) =>
    AttributeValue.Number(BigDecimal(a.toString))
  implicit val shortSetToAttributeValue: ToAttributeValue[Set[Short]]           = (a: Set[Short]) =>
    AttributeValue.NumberSet(a.map(s => BigDecimal(s.toString)))

  // Int support
  implicit val intToAttributeValue: ToAttributeValue[Int]               = (a: Int) => AttributeValue.Number(BigDecimal(a))
  implicit val intSetToAttributeValue: ToAttributeValue[Set[Int]]       = (a: Set[Int]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Long support
  implicit val longToAttributeValue: ToAttributeValue[Long]             = (a: Long) => AttributeValue.Number(BigDecimal(a))
  implicit val longSetToAttributeValue: ToAttributeValue[Set[Long]]     = (a: Set[Long]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Double support
  implicit val doubleToAttributeValue: ToAttributeValue[Double]         = (a: Double) => AttributeValue.Number(BigDecimal(a))
  implicit val doubleSetToAttributeValue: ToAttributeValue[Set[Double]] = (a: Set[Double]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Float support
  implicit val floatToAttributeValue: ToAttributeValue[Float]           = (a: Float) =>
    AttributeValue.Number(BigDecimal.decimal(a))
  implicit val floatSetToAttributeValue: ToAttributeValue[Set[Float]]   = (a: Set[Float]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.decimal))

}

trait ToAttributeValueLowPriorityImplicits0 extends ToAttributeValueLowPriorityImplicits1 {
  implicit def collectionToAttributeValue[Col[X] <: Iterable[X], A](implicit
    element: ToAttributeValue[A]
  ): ToAttributeValue[
    Col[A]
  ] = (xs: Col[A]) => AttributeValue.List(Chunk.fromIterable(xs.map(element.toAttributeValue)))

}

trait ToAttributeValueLowPriorityImplicits1 {
  implicit val nullToAttributeValue: ToAttributeValue[Null] = (_: Null) => AttributeValue.Null
}
