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
import zio.test._

object FromAttributeValueSpec extends ZIOSpecDefault {

  private def from[A: FromAttributeValue](av: AttributeValue): Either[DynamoDBError.ItemError, A] =
    FromAttributeValue[A].fromAttributeValue(av)

  def spec = suite("FromAttributeValue")(
    suite("String")(
      test("decodes AttributeValue.String") {
        assertTrue(from[String](AttributeValue.String("hello")) == Right("hello"))
      },
      test("error on wrong type") {
        assertTrue(from[String](AttributeValue.Number(BigDecimal(1))).isLeft)
      }
    ),
    suite("Boolean")(
      test("decodes AttributeValue.Bool true") {
        assertTrue(from[Boolean](AttributeValue.Bool(true)) == Right(true))
      },
      test("decodes AttributeValue.Bool false") {
        assertTrue(from[Boolean](AttributeValue.Bool(false)) == Right(false))
      },
      test("error on wrong type") {
        assertTrue(from[Boolean](AttributeValue.String("true")).isLeft)
      }
    ),
    suite("Int")(
      test("decodes AttributeValue.Number") {
        assertTrue(from[Int](AttributeValue.Number(BigDecimal(42))) == Right(42))
      },
      test("error on wrong type") {
        assertTrue(from[Int](AttributeValue.String("42")).isLeft)
      }
    ),
    suite("Long")(
      test("decodes AttributeValue.Number") {
        assertTrue(from[Long](AttributeValue.Number(BigDecimal(100L))) == Right(100L))
      },
      test("error on wrong type") {
        assertTrue(from[Long](AttributeValue.String("100")).isLeft)
      }
    ),
    suite("Double")(
      test("decodes AttributeValue.Number") {
        assertTrue(from[Double](AttributeValue.Number(BigDecimal(1.5))) == Right(1.5))
      },
      test("error on wrong type") {
        assertTrue(from[Double](AttributeValue.String("1.5")).isLeft)
      }
    ),
    suite("Float")(
      test("decodes AttributeValue.Number") {
        val result = from[Float](AttributeValue.Number(BigDecimal(1.5)))
        assertTrue(result.isRight)
      },
      test("error on wrong type") {
        assertTrue(from[Float](AttributeValue.String("1.5")).isLeft)
      }
    ),
    suite("Short")(
      test("decodes AttributeValue.Number") {
        assertTrue(from[Short](AttributeValue.Number(BigDecimal(5))) == Right(5.toShort))
      },
      test("error on wrong type") {
        assertTrue(from[Short](AttributeValue.String("5")).isLeft)
      }
    ),
    suite("BigDecimal")(
      test("decodes AttributeValue.Number") {
        assertTrue(from[BigDecimal](AttributeValue.Number(BigDecimal(99))) == Right(BigDecimal(99)))
      },
      test("error on wrong type") {
        assertTrue(from[BigDecimal](AttributeValue.String("99")).isLeft)
      }
    ),
    suite("Binary (Iterable[Byte])")(
      test("decodes AttributeValue.Binary") {
        val bytes = List(1.toByte, 2.toByte)
        assertTrue(from[Iterable[Byte]](AttributeValue.Binary(bytes)).map(_.toList) == Right(bytes))
      },
      test("error on wrong type") {
        assertTrue(from[Iterable[Byte]](AttributeValue.String("x")).isLeft)
      }
    ),
    suite("Byte")(
      test("decodes first byte from AttributeValue.Binary") {
        assertTrue(from[Byte](AttributeValue.Binary(List(42.toByte))) == Right(42.toByte))
      },
      test("error on empty binary") {
        assertTrue(from[Byte](AttributeValue.Binary(List.empty)).isLeft)
      },
      test("error on wrong type") {
        assertTrue(from[Byte](AttributeValue.String("x")).isLeft)
      }
    ),
    suite("BinarySet (Iterable[Iterable[Byte]])")(
      test("decodes AttributeValue.BinarySet") {
        val bs = List(List(1.toByte), List(2.toByte))
        assertTrue(from[Iterable[Iterable[Byte]]](AttributeValue.BinarySet(bs)).isRight)
      },
      test("error on wrong type") {
        assertTrue(from[Iterable[Iterable[Byte]]](AttributeValue.String("x")).isLeft)
      }
    ),
    suite("Set[String]")(
      test("decodes AttributeValue.StringSet") {
        assertTrue(from[Set[String]](AttributeValue.StringSet(Set("a", "b"))) == Right(Set("a", "b")))
      },
      test("error on wrong type") {
        assertTrue(from[Set[String]](AttributeValue.String("a")).isLeft)
      }
    ),
    suite("Set[Int]")(
      test("decodes AttributeValue.NumberSet") {
        assertTrue(from[Set[Int]](AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2)))) == Right(Set(1, 2)))
      },
      test("error on wrong type") {
        assertTrue(from[Set[Int]](AttributeValue.String("1")).isLeft)
      }
    ),
    suite("Set[Long]")(
      test("decodes AttributeValue.NumberSet") {
        assertTrue(from[Set[Long]](AttributeValue.NumberSet(Set(BigDecimal(1L), BigDecimal(2L)))) == Right(Set(1L, 2L)))
      },
      test("error on wrong type") {
        assertTrue(from[Set[Long]](AttributeValue.String("1")).isLeft)
      }
    ),
    suite("Set[Double]")(
      test("decodes AttributeValue.NumberSet") {
        assertTrue(from[Set[Double]](AttributeValue.NumberSet(Set(BigDecimal(1.0)))).isRight)
      },
      test("error on wrong type") {
        assertTrue(from[Set[Double]](AttributeValue.String("1.0")).isLeft)
      }
    ),
    suite("Set[Float]")(
      test("decodes AttributeValue.NumberSet") {
        assertTrue(from[Set[Float]](AttributeValue.NumberSet(Set(BigDecimal(1.0)))).isRight)
      },
      test("error on wrong type") {
        assertTrue(from[Set[Float]](AttributeValue.String("1.0")).isLeft)
      }
    ),
    suite("Set[Short]")(
      test("decodes AttributeValue.NumberSet") {
        assertTrue(from[Set[Short]](AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2)))).isRight)
      },
      test("error on wrong type") {
        assertTrue(from[Set[Short]](AttributeValue.String("1")).isLeft)
      }
    ),
    suite("Set[BigDecimal]")(
      test("decodes AttributeValue.NumberSet") {
        assertTrue(from[Set[BigDecimal]](AttributeValue.NumberSet(Set(BigDecimal(1)))) == Right(Set(BigDecimal(1))))
      },
      test("error on wrong type") {
        assertTrue(from[Set[BigDecimal]](AttributeValue.String("1")).isLeft)
      }
    ),
    suite("Option[A]")(
      test("AttributeValue.Null decodes as None") {
        assertTrue(from[Option[String]](AttributeValue.Null) == Right(None))
      },
      test("non-null value decodes as Some") {
        assertTrue(from[Option[String]](AttributeValue.String("x")) == Right(Some("x")))
      },
      test("wrong type inside Some produces Left") {
        assertTrue(from[Option[Int]](AttributeValue.String("x")).isLeft)
      }
    ),
    suite("Iterable[A]")(
      test("error on non-list type") {
        assertTrue(from[Iterable[Int]](AttributeValue.String("x")).isLeft)
      }
    ),
    suite("AttrMap")(
      test("decodes AttributeValue.Map into AttrMap") {
        val map    = Map(AttributeValue.String("id") -> AttributeValue.String("1"))
        val result = from[AttrMap](AttributeValue.Map(map))
        assertTrue(result.map(_.map.get("id")).contains(Some(AttributeValue.String("1"))))
      },
      test("decodes a JMapView-backed AttributeValue.Map (the shape the codec encoder produces)") {
        val backing = JMapView.hash
          .builder[AttributeValue.String, AttributeValue]
          .addOne(AttributeValue.String("id"), AttributeValue.String("1"))
          .addOne(AttributeValue.String("age"), AttributeValue.Number(BigDecimal(30)))
          .result
        val result  = from[AttrMap](AttributeValue.Map(backing))
        assertTrue(
          result.map(_.map.get("id")).contains(Some(AttributeValue.String("1"))),
          result.map(_.map.get("age")).contains(Some(AttributeValue.Number(BigDecimal(30)))),
          result.map(_.map.size).contains(2),
          // the fast path wraps the unwrapped keys straight into a JMapView, no immutable Map copy
          result.exists(_.map.isInstanceOf[JMapView[_, _]])
        )
      },
      test("error on wrong type") {
        assertTrue(from[AttrMap](AttributeValue.String("x")).isLeft)
      }
    ),
    suite("Map[String, A]")(
      test("error on wrong type for Map[String, String]") {
        assertTrue(from[Map[String, String]](AttributeValue.String("x")).isLeft)
      }
    )
  )
}
