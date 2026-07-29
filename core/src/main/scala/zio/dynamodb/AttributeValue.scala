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

import scala.collection.immutable.{ Map => SMap, Set }
import scala.util.Try

sealed trait AttributeValue { self =>
  private[dynamodb] final val showType: String =
    self match {
      case _: AttributeValue.Binary    => "AttributeValue.Binary"
      case _: AttributeValue.BinarySet => "AttributeValue.BinarySet"
      case _: AttributeValue.Bool      => "AttributeValue.Bool"
      case _: AttributeValue.List      => "AttributeValue.List"
      case _: AttributeValue.Map       => "AttributeValue.Map"
      case _: AttributeValue.Number    => "AttributeValue.Number"
      case _: AttributeValue.NumberSet => "AttributeValue.NumberSet"
      case _: AttributeValue.Null.type => "AttributeValue.Null"
      case _: AttributeValue.String    => "AttributeValue.String"
      case _: AttributeValue.StringSet => "AttributeValue.StringSet"
    }
}
object AttributeValue       {
  import Predef.{ String => ScalaString }
  import scala.collection.immutable.{ Map => ScalaMap }

  private[dynamodb] final class Binary(val value: Array[Byte])                    extends AttributeValue {
    override def equals(that: Any): Boolean = that match {
      case b: Binary => java.util.Arrays.equals(value, b.value)
      case _         => false
    }
    override def hashCode: Int              = java.util.Arrays.hashCode(value)
    override def toString: ScalaString      = s"Binary(${java.util.Arrays.toString(value)})"
  }
  private[dynamodb] object Binary {
    def apply(value: Array[Byte]): Binary       = new Binary(value)
    def apply(value: Iterable[Byte]): Binary    = new Binary(value.toArray)
    def unapply(b: Binary): Option[Array[Byte]] = Some(b.value)

    // Produces the ZIO DynamoDB / schema1 List-of-Number representation for migration tooling.
    def toListOfNumbers(bytes: Array[Byte]): AttributeValue.List = {
      val avs = new Array[AttributeValue](bytes.length)
      var i   = 0
      while (i < bytes.length) { avs(i) = AttributeValue.Number(BigDecimal.valueOf(bytes(i).toLong)); i += 1 }
      AttributeValue.List(scala.collection.immutable.ArraySeq.unsafeWrapArray(avs))
    }
  }
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]])   extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                         extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])        extends AttributeValue { self =>
    def +(av: AttributeValue): List = List(self.value ++ Iterable(av))
  }
  private[dynamodb] final case class Number(value: BigDecimal)                    extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue { self =>
    def +(s: ScalaString): Either[ScalaString, NumberSet] =
      Try(BigDecimal(s)).toEither.left.map(_.getMessage).map(n => NumberSet(self.value + n))
  }
  private[dynamodb] object NumberSet {
    val empty: NumberSet = NumberSet(Set.empty)
  }
  private[dynamodb] case object Null                                              extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)                   extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString])           extends AttributeValue { self =>
    def +(s: ScalaString): StringSet = StringSet(self.value + s)
  }
  private[dynamodb] object StringSet {
    val empty: StringSet = StringSet(Set.empty)
  }
  private[dynamodb] object List {
    val empty: List = List(Iterable.empty)
  }
  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue { self =>
    def +(t: (ScalaString, AttributeValue)): Map = {
      val (s, av) = t
      Map(self.value + ((String(s), av)))
    }
  }
  object Map {
    val empty: Map = Map(ScalaMap.empty)
  }
}
