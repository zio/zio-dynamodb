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
import zio.test._
import zio.test.Assertion.{ equalTo, hasField, isSome, isSubtype }

object ToAttributeValueSpec extends ZIOSpecDefault {

  private def to[A: ToAttributeValue](a: A): AttributeValue = ToAttributeValue[A].toAttributeValue(a)

  def spec = suite("ToAttributeValue")(
    suite("primitive types")(
      test("String") {
        assertTrue(to("hello") == AttributeValue.String("hello"))
      },
      test("Boolean true") {
        assertTrue(to(true) == AttributeValue.Bool(true))
      },
      test("Boolean false") {
        assertTrue(to(false) == AttributeValue.Bool(false))
      },
      test("Int") {
        assertTrue(to(42) == AttributeValue.Number(BigDecimal(42)))
      },
      test("Long") {
        assertTrue(to(42L) == AttributeValue.Number(BigDecimal(42L)))
      },
      test("Double") {
        assertTrue(to(1.5) == AttributeValue.Number(BigDecimal(1.5)))
      },
      test("Float") {
        assertTrue(to(1.5f) == AttributeValue.Number(BigDecimal.decimal(1.5f)))
      },
      test("Short") {
        assertTrue(to(5.toShort) == AttributeValue.Number(BigDecimal("5")))
      },
      test("BigDecimal") {
        assertTrue(to(BigDecimal(99)) == AttributeValue.Number(BigDecimal(99)))
      },
      test("Byte via Chunk") {
        assertTrue(to(42.toByte) == AttributeValue.Binary(Chunk(42.toByte)))
      },
      test("Null") {
        assertTrue(to[Null](null) == AttributeValue.Null)
      }
    ),
    suite("Option")(
      test("Some(value) encodes the inner value") {
        assertTrue(to(Some("hello"): Option[String]) == AttributeValue.String("hello"))
      },
      test("None encodes as AttributeValue.Null") {
        assertTrue(to(Option.empty[String]) == AttributeValue.Null)
      }
    ),
    suite("Set types")(
      test("Set[String]") {
        assertTrue(to(Set("a", "b")) == AttributeValue.StringSet(Set("a", "b")))
      },
      test("Set[Int]") {
        assertTrue(to(Set(1, 2)) == AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
      },
      test("Set[Long]") {
        assertTrue(to(Set(1L, 2L)) == AttributeValue.NumberSet(Set(BigDecimal(1L), BigDecimal(2L))))
      },
      test("Set[Double]") {
        assertTrue(to(Set(1.0, 2.0)) == AttributeValue.NumberSet(Set(BigDecimal(1.0), BigDecimal(2.0))))
      },
      test("Set[Float]") {
        assertTrue(to(Set(1.0f)) == AttributeValue.NumberSet(Set(BigDecimal.decimal(1.0f))))
      },
      test("Set[Short]") {
        val s: Set[Short] = Set(1.toShort, 2.toShort)
        assertTrue(to(s) == AttributeValue.NumberSet(s.map(sh => BigDecimal(sh.toString))))
      },
      test("Set[BigDecimal]") {
        assertTrue(to(Set(BigDecimal(1), BigDecimal(2))) == AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
      }
    ),
    suite("collection types")(
      test("List[String] becomes AttributeValue.List") {
        assert(to(List("a", "b")))(
          isSubtype[AttributeValue.List](
            hasField(
              "value",
              _.value.toList,
              equalTo(List[AttributeValue](AttributeValue.String("a"), AttributeValue.String("b")))
            )
          )
        )
      },
      test("Iterable[Int] becomes AttributeValue.List") {
        assert(to(Iterable(1, 2)))(
          isSubtype[AttributeValue.List](
            hasField(
              "value",
              _.value.toList,
              equalTo(List[AttributeValue](AttributeValue.Number(BigDecimal(1)), AttributeValue.Number(BigDecimal(2))))
            )
          )
        )
      }
    ),
    suite("AttrMap")(
      test("AttrMap becomes AttributeValue.Map with AttributeValue.String keys") {
        val m = Item("id" -> "1", "n" -> 42)
        assert(to(m))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              (m: AttributeValue.Map) => m.value.get(AttributeValue.String("id")),
              isSome(equalTo(AttributeValue.String("1"): AttributeValue))
            ) &&
              hasField(
                "value",
                (m: AttributeValue.Map) => m.value.get(AttributeValue.String("n")),
                isSome(equalTo(AttributeValue.Number(BigDecimal(42)): AttributeValue))
              )
          )
        )
      }
    ),
    suite("Map[String, A]")(
      test("Map[String, Int] becomes AttributeValue.Map") {
        val m = Map("a" -> 1, "b" -> 2)
        assert(to(m))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              (m: AttributeValue.Map) => m.value.get(AttributeValue.String("a")),
              isSome(equalTo(AttributeValue.Number(BigDecimal(1)): AttributeValue))
            ) &&
              hasField(
                "value",
                (m: AttributeValue.Map) => m.value.get(AttributeValue.String("b")),
                isSome(equalTo(AttributeValue.Number(BigDecimal(2)): AttributeValue))
              )
          )
        )
      }
    ),
    suite("AttributeValue identity")(
      test("AttributeValue passes through unchanged") {
        val av: AttributeValue = AttributeValue.String("x")
        assertTrue(ToAttributeValue[AttributeValue].toAttributeValue(av) == av)
      }
    )
  )
}
