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

import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import Utils.ListUtils

import java.util.{ HashMap => JHashMap }

/**
 * The decode-side counterpart to [[ToAttributeValue]] — converts a wire-level
 * [[AttributeValue]] back into a Scala value, used by the Low-Level API's item-reading
 * accessors. Instances for every primitive, `Option`, `Iterable`, `Map[String, _]`, and
 * native DynamoDB set type are provided implicitly; write a custom instance for an
 * application-specific type rather than decoding to a supported type and converting after.
 */
trait FromAttributeValue[+A] {
  def fromAttributeValue(av: AttributeValue): Either[DecodingError, A]
}

/** Built-in [[FromAttributeValue]] instances for primitives, collections, and `Option`. */
object FromAttributeValue {

  def apply[A](implicit from: FromAttributeValue[A]): FromAttributeValue[A] = from

  implicit def optionFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Option[A]] = {
    case AttributeValue.Null =>
      Right(None)
    case av: AttributeValue  =>
      ev.fromAttributeValue(av).map(Some(_))
  }

  implicit val binaryFromAttributeValue: FromAttributeValue[Iterable[Byte]] = {
    case AttributeValue.Binary(b) => Right(b)
    case av                       =>
      Left(DecodingError(s"Error getting binary value. Expected AttributeValue.Binary but found ${av.showType}"))
  }

  implicit val byteFromAttributeValue: FromAttributeValue[Byte] = {
    case AttributeValue.Binary(b) => b.headOption.toRight(DecodingError("Error: byte array is empty"))
    case av                       => Left(DecodingError(s"Error getting byte value. Expected AttributeValue.Binary but found ${av.showType}"))
  }

  implicit def binarySetFromAttributeValue: FromAttributeValue[Iterable[Iterable[Byte]]] = {
    case AttributeValue.BinarySet(set) => Right(set)
    case av                            =>
      Left(DecodingError(s"Error getting binary set value. Expected AttributeValue.BinarySet but found ${av.showType}"))
  }

  implicit val booleanFromAttributeValue: FromAttributeValue[Boolean] = {
    case AttributeValue.Bool(b) => Right(b)
    case av                     =>
      Left(DecodingError(s"Error getting boolean value. Expected AttributeValue.Bool but found ${av.showType}"))
  }

  implicit val stringFromAttributeValue: FromAttributeValue[String] = {
    case AttributeValue.String(s) => Right(s)
    case av                       =>
      Left(DecodingError(s"Error getting string value. Expected AttributeValue.String but found ${av.showType}"))
  }

  implicit val shortFromAttributeValue: FromAttributeValue[Short]                   = {
    case AttributeValue.Number(bd) => Right(bd.shortValue)
    case av                        =>
      Left(DecodingError(s"Error getting short value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val shortSetFromAttributeValue: FromAttributeValue[Set[Short]]           = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.shortValue))
    case av                              =>
      Left(DecodingError(s"Error getting short set value. Expected AttributeValue.NumberSet but found ${av.showType}"))
  }
  implicit val intFromAttributeValue: FromAttributeValue[Int]                       = {
    case AttributeValue.Number(bd) => Right(bd.intValue)
    case av                        => Left(DecodingError(s"Error getting int value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val intSetFromAttributeValue: FromAttributeValue[Set[Int]]               = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.intValue))
    case av                              =>
      Left(DecodingError(s"Error getting int set value. Expected AttributeValue.NumberSet but found ${av.showType}"))
  }
  implicit val longFromAttributeValue: FromAttributeValue[Long]                     = {
    case AttributeValue.Number(bd) => Right(bd.longValue)
    case av                        => Left(DecodingError(s"Error getting long value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val longSetFromAttributeValue: FromAttributeValue[Set[Long]]             = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.longValue))
    case av                              =>
      Left(DecodingError(s"Error getting long set value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val floatFromAttributeValue: FromAttributeValue[Float]                   = {
    case AttributeValue.Number(bd) => Right(bd.floatValue)
    case av                        =>
      Left(DecodingError(s"Error getting float value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val floatSetFromAttributeValue: FromAttributeValue[Set[Float]]           = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.floatValue))
    case av                              =>
      Left(DecodingError(s"Error getting float set value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val doubleFromAttributeValue: FromAttributeValue[Double]                 = {
    case AttributeValue.Number(bd) => Right(bd.doubleValue)
    case av                        =>
      Left(DecodingError(s"Error getting double value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val doubleSetFromAttributeValue: FromAttributeValue[Set[Double]]         = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.doubleValue))
    case av                              =>
      Left(DecodingError(s"Error getting double value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val bigDecimalFromAttributeValue: FromAttributeValue[BigDecimal]         = {
    case AttributeValue.Number(bd) => Right(bd)
    case av                        =>
      Left(DecodingError(s"Error getting BigDecimal value. Expected AttributeValue.Number but found ${av.showType}"))
  }
  implicit val bigDecimalSetFromAttributeValue: FromAttributeValue[Set[BigDecimal]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet)
    case av                              =>
      Left(
        DecodingError(s"Error getting BigDecimal set value. Expected AttributeValue.Number but found ${av.showType}")
      )
  }

  implicit def mapFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Map[String, A]] = {
    case AttributeValue.Map(map) =>
      map.toList.forEach { case (avK, avV) =>
        ev.fromAttributeValue(avV).map(v => (avK.value, v))
      }
        .map(_.toMap)
    case av                      => Left(DecodingError(s"Error getting map value. Expected AttributeValue.Map but found ${av.showType}"))
  }

  implicit def stringSetFromAttributeValue: FromAttributeValue[Set[String]] = {
    case AttributeValue.StringSet(set) => Right(set)
    case av                            =>
      Left(DecodingError(s"Error getting string set value. Expected AttributeValue.StringSet but found ${av.showType}"))
  }

  implicit val attrMapFromAttributeValue: FromAttributeValue[AttrMap] = {
    case AttributeValue.Map(map) =>
      // Unwrap the AttributeValue.String keys to plain String in a single pass into a
      // JHashMap, then wrap it (no copy) via AttrMap.fromJavaMap — rather than building
      // two throwaway immutable maps (`.toMap` then `.map`).
      val jm = new JHashMap[String, AttributeValue](map.size * 2)
      map match {
        case jmap: JMapView[_, _] =>
          val it = jmap.underlying.entrySet().iterator()
          while (it.hasNext) {
            val e = it.next()
            jm.put(e.getKey.asInstanceOf[AttributeValue.String].value, e.getValue.asInstanceOf[AttributeValue])
          }
        case m                    =>
          m.foreach { case (k, v) => jm.put(k.value, v) }
      }
      Right(AttrMap.fromJavaMap(jm))
    case av                      => Left(DecodingError(s"Error getting AttrMap value. Expected AttributeValue.Map but found ${av.showType}"))
  }

  implicit def iterableFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Iterable[A]] = {
    case AttributeValue.List(list) =>
      list.forEach(ev.fromAttributeValue)
    case av                        =>
      Left(DecodingError(s"Error getting iterable value. Expected AttributeValue.List but found ${av.showType}"))
  }

}
