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

import zio.blocks.schema.{ Modifier, Schema }
import zio.blocks.schema.NameMapper
import zio.dynamodb.AttributeValue
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.test._
import zio.test.Assertion.{ equalTo, hasField, isFalse, isSome, isSubtype, isTrue }

object DerivationConfigureSpec extends ZIOSpecDefault {

  private def deriveCodec[A: Schema](implicit cfg: DynamoDBCodecDeriverConfigure[A]): DynamoDBCodec[A] =
    Schema[A].deriving(cfg.configure(DynamoDBCodecDeriver)).derive

  // ── global deriver settings via fieldNameMapper ────────────────────────────

  private case class Person(id: String, name: String, age: Int)
  private object Person {
    implicit val schema: Schema[Person]                     = Schema.derived
    implicit val cfg: DynamoDBCodecDeriverConfigure[Person] =
      (d: DynamoDBCodecDeriver) =>
        d.withFieldNameMapper(NameMapper.Custom {
          case "name" => "fullName"
          case other  => other
        })
  }

  private val alice = Person("1", "Alice", 30)

  // ── per-field modifier override via withModifier ───────────────────────────

  private case class Contact(id: String, name: String)
  private object Contact {
    implicit val schema: Schema[Contact]                     = Schema.derived
    implicit val cfg: DynamoDBCodecDeriverConfigure[Contact] =
      d => d.withModifier(schema.reflect.typeId, "name", Modifier.rename("fullName"))
  }

  private val bob = Contact("2", "Bob")

  // ── variant-level deriver settings via caseNameMapper ─────────────────────

  private sealed trait Shape
  private case class Circle(radius: Double)                   extends Shape
  private case class Rectangle(width: Double, height: Double) extends Shape
  private object Shape {
    implicit val schema: Schema[Shape]                     = Schema.derived
    implicit val cfg: DynamoDBCodecDeriverConfigure[Shape] =
      d =>
        d.withCaseNameMapper(NameMapper.Custom {
          case "Circle"    => "circle"
          case "Rectangle" => "rectangle"
          case other       => other
        })
  }

  def spec = suite("DynamoDBCodecDeriverConfigure")(
    suite("global deriver settings — fieldNameMapper")(
      test("renamed field appears under new key in encoded item") {
        val codec = deriveCodec[Person]
        assert(codec.encoder(alice))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("fullName")),
              isSome(equalTo(AttributeValue.String("Alice")))
            ) &&
              hasField[AttributeValue.Map, Boolean](
                "value",
                _.value.contains(AttributeValue.String("name")),
                isFalse
              )
          )
        )
      },
      test("unrenamed fields are unaffected") {
        val codec = deriveCodec[Person]
        assert(codec.encoder(alice))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("id")),
              isSome(equalTo(AttributeValue.String("1")))
            ) &&
              hasField[AttributeValue.Map, Option[AttributeValue]](
                "value",
                _.value.get(AttributeValue.String("age")),
                isSome(equalTo(AttributeValue.Number(BigDecimal.valueOf(30L))))
              )
          )
        )
      },
      test("decode reads from renamed key") {
        val codec = deriveCodec[Person]
        val item  = AttributeValue.Map(
          Map(
            AttributeValue.String("id")       -> AttributeValue.String("1"),
            AttributeValue.String("fullName") -> AttributeValue.String("Alice"),
            AttributeValue.String("age")      -> AttributeValue.Number(BigDecimal.valueOf(30L))
          )
        )
        assertTrue(codec.decoder(item) == Right(alice))
      },
      test("round-trip encode → decode preserves value") {
        val codec = deriveCodec[Person]
        assertTrue(codec.decoder(codec.encoder(alice)) == Right(alice))
      },
      test("decode fails when item uses original field name") {
        val codec        = deriveCodec[Person]
        val wrongKeyItem = AttributeValue.Map(
          Map(
            AttributeValue.String("id")   -> AttributeValue.String("1"),
            AttributeValue.String("name") -> AttributeValue.String("Alice"),
            AttributeValue.String("age")  -> AttributeValue.Number(BigDecimal.valueOf(30L))
          )
        )
        assertTrue(codec.decoder(wrongKeyItem).isLeft)
      }
    ),
    suite("per-field modifier override — withModifier")(
      test("renamed field appears under new key in encoded item") {
        val codec = deriveCodec[Contact]
        assert(codec.encoder(bob))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("fullName")),
              isSome(equalTo(AttributeValue.String("Bob")))
            ) &&
              hasField[AttributeValue.Map, Boolean](
                "value",
                _.value.contains(AttributeValue.String("name")),
                isFalse
              )
          )
        )
      },
      test("unrenamed fields are unaffected") {
        val codec = deriveCodec[Contact]
        assert(codec.encoder(bob))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("id")),
              isSome(equalTo(AttributeValue.String("2")))
            )
          )
        )
      },
      test("decode reads from renamed key") {
        val codec = deriveCodec[Contact]
        val item  = AttributeValue.Map(
          Map(
            AttributeValue.String("id")       -> AttributeValue.String("2"),
            AttributeValue.String("fullName") -> AttributeValue.String("Bob")
          )
        )
        assertTrue(codec.decoder(item) == Right(bob))
      },
      test("round-trip encode → decode preserves value") {
        val codec = deriveCodec[Contact]
        assertTrue(codec.decoder(codec.encoder(bob)) == Right(bob))
      }
    ),
    suite("variant-level deriver settings — caseNameMapper")(
      test("discriminator key uses mapped case name") {
        val codec = deriveCodec[Shape]
        assert(codec.encoder(Circle(1.0)))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Boolean](
              "value",
              _.value.contains(AttributeValue.String("circle")),
              isTrue
            ) &&
              hasField[AttributeValue.Map, Boolean](
                "value",
                _.value.contains(AttributeValue.String("Circle")),
                isFalse
              )
          )
        )
      },
      test("decode reads from mapped case name") {
        val codec = deriveCodec[Shape]
        val item  = AttributeValue.Map(
          Map(
            AttributeValue.String("rectangle") -> AttributeValue.Map(
              Map(
                AttributeValue.String("width")  -> AttributeValue.Number(BigDecimal(3.0)),
                AttributeValue.String("height") -> AttributeValue.Number(BigDecimal(4.0))
              )
            )
          )
        )
        assertTrue(codec.decoder(item) == Right(Rectangle(3.0, 4.0)))
      },
      test("round-trip encode → decode preserves value") {
        val codec        = deriveCodec[Shape]
        val shape: Shape = Rectangle(3.0, 4.0)
        assertTrue(codec.decoder(codec.encoder(shape)) == Right(shape))
      }
    )
  )
}
