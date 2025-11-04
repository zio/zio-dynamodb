package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, Codec, DynamoDBQuery, Item }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.{ equalTo, isRight }
import zio.test._
import zio.NonEmptyChunk
import zio.prelude.NonEmptySet

object NonEmptySequenceCodecSpec extends ZIOSpecDefault {

  final case class CaseClassOfNonEmptyChunk(nums: NonEmptyChunk[Int])
  object CaseClassOfNonEmptyChunk {
    implicit val schema: Schema[CaseClassOfNonEmptyChunk] = DeriveSchema.gen[CaseClassOfNonEmptyChunk]
  }

  final case class CaseClassOfNonEmptyChunkOfCaseClass(elements: NonEmptyChunk[SimpleCaseClass])
  object CaseClassOfNonEmptyChunkOfCaseClass {
    implicit val schema: Schema[CaseClassOfNonEmptyChunkOfCaseClass] =
      DeriveSchema.gen[CaseClassOfNonEmptyChunkOfCaseClass]
  }

  final case class CaseClassOfNonEmptySet(tags: NonEmptySet[String])
  object CaseClassOfNonEmptySet {
    implicit val schema: Schema[CaseClassOfNonEmptySet] = DeriveSchema.gen[CaseClassOfNonEmptySet]
  }

  final case class CaseClassOfNonEmptySetOfInt(nums: NonEmptySet[Int])
  object CaseClassOfNonEmptySetOfInt {
    implicit val schema: Schema[CaseClassOfNonEmptySetOfInt] = DeriveSchema.gen[CaseClassOfNonEmptySetOfInt]
  }

  final case class SimpleCaseClass(id: Int, name: String)
  object SimpleCaseClass {
    implicit val schema: Schema[SimpleCaseClass] = DeriveSchema.gen[SimpleCaseClass]
  }

  override def spec: Spec[zio.test.TestEnvironment, Any] =
    suite("NonEmptySequence codecs")(
      suite("when encoding NonEmptyChunk")(
        test("encodes NonEmptyChunk of Int") {
          val actual: AttributeValue =
            Codec.encoder(CaseClassOfNonEmptyChunk.schema)(
              CaseClassOfNonEmptyChunk(NonEmptyChunk(1, 2, 3))
            )

          assert(actual.toString)(equalTo("Map(Map(String(nums) -> List(Chunk(Number(1),Number(2),Number(3)))))"))
        },
        test("encodes NonEmptyChunk with single element") {
          val actual: AttributeValue =
            Codec.encoder(CaseClassOfNonEmptyChunk.schema)(
              CaseClassOfNonEmptyChunk(NonEmptyChunk(42))
            )

          assert(actual.toString)(equalTo("Map(Map(String(nums) -> List(Chunk(Number(42)))))"))
        },
        test("encodes NonEmptyChunk of case class") {
          val expectedItem: Item = Item("elements" -> List(Item("id" -> 1, "name" -> "Alice")))

          val item = DynamoDBQuery.toItem(
            CaseClassOfNonEmptyChunkOfCaseClass(
              NonEmptyChunk(SimpleCaseClass(1, "Alice"))
            )
          )

          assert(item)(equalTo(expectedItem))
        },
        test("encodes NonEmptyChunk of multiple case classes") {
          val expectedItem: Item = Item(
            "elements" -> List(
              Item("id" -> 1, "name" -> "Alice"),
              Item("id" -> 2, "name" -> "Bob")
            )
          )

          val item = DynamoDBQuery.toItem(
            CaseClassOfNonEmptyChunkOfCaseClass(
              NonEmptyChunk(SimpleCaseClass(1, "Alice"), SimpleCaseClass(2, "Bob"))
            )
          )

          assert(item)(equalTo(expectedItem))
        }
      ),
      suite("when decoding NonEmptyChunk")(
        test("decodes NonEmptyChunk of Int") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("nums") -> AttributeValue.List(
                List(
                  AttributeValue.Number(BigDecimal(1)),
                  AttributeValue.Number(BigDecimal(2)),
                  AttributeValue.Number(BigDecimal(3))
                )
              )
            )
          )

          val actual = Codec.decoder(CaseClassOfNonEmptyChunk.schema)(av)

          assert(actual)(isRight(equalTo(CaseClassOfNonEmptyChunk(NonEmptyChunk(1, 2, 3)))))
        },
        test("decodes NonEmptyChunk with single element") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("nums") -> AttributeValue.List(
                List(AttributeValue.Number(BigDecimal(42)))
              )
            )
          )

          val actual = Codec.decoder(CaseClassOfNonEmptyChunk.schema)(av)

          assert(actual)(isRight(equalTo(CaseClassOfNonEmptyChunk(NonEmptyChunk(42)))))
        },
        test("decodes NonEmptyChunk of case class") {
          val item = Item("elements" -> List(Item("id" -> 1, "name" -> "Alice")))

          val actual = DynamoDBQuery.fromItem[CaseClassOfNonEmptyChunkOfCaseClass](item)

          assert(actual)(
            isRight(
              equalTo(
                CaseClassOfNonEmptyChunkOfCaseClass(
                  NonEmptyChunk(SimpleCaseClass(1, "Alice"))
                )
              )
            )
          )
        },
        test("decodes NonEmptyChunk of multiple case classes") {
          val item = Item(
            "elements" -> List(
              Item("id" -> 1, "name" -> "Alice"),
              Item("id" -> 2, "name" -> "Bob")
            )
          )

          val actual = DynamoDBQuery.fromItem[CaseClassOfNonEmptyChunkOfCaseClass](item)

          assert(actual)(
            isRight(
              equalTo(
                CaseClassOfNonEmptyChunkOfCaseClass(
                  NonEmptyChunk(SimpleCaseClass(1, "Alice"), SimpleCaseClass(2, "Bob"))
                )
              )
            )
          )
        }
      ),
      suite("when encoding NonEmptySet")(
        test("encodes NonEmptySet of String as native StringSet") {
          val expectedItem: Item = Item("tags" -> Set("tag1", "tag2"))

          val item = DynamoDBQuery.toItem(
            CaseClassOfNonEmptySet(NonEmptySet("tag1", "tag2"))
          )

          assert(item)(equalTo(expectedItem))
        },
        test("encodes NonEmptySet of String with single element") {
          val actual: AttributeValue =
            Codec.encoder(CaseClassOfNonEmptySet.schema)(
              CaseClassOfNonEmptySet(NonEmptySet("only-tag"))
            )

          assert(actual.toString)(equalTo("Map(Map(String(tags) -> StringSet(Set(only-tag))))"))
        },
        test("encodes NonEmptySet of Int as native NumberSet") {
          val expectedItem: Item = Item("nums" -> Set(1, 2))

          val item = DynamoDBQuery.toItem(
            CaseClassOfNonEmptySetOfInt(NonEmptySet(1, 2))
          )

          assert(item)(equalTo(expectedItem))
        },
        test("encodes NonEmptySet of Int with single element") {
          val actual: AttributeValue =
            Codec.encoder(CaseClassOfNonEmptySetOfInt.schema)(
              CaseClassOfNonEmptySetOfInt(NonEmptySet(42))
            )

          assert(actual.toString)(equalTo("Map(Map(String(nums) -> NumberSet(Set(42))))"))
        }
      ),
      suite("when decoding NonEmptySet")(
        test("decodes NonEmptySet of String from native StringSet") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("tags") -> AttributeValue.StringSet(Set("tag1", "tag2"))
            )
          )

          val actual = Codec.decoder(CaseClassOfNonEmptySet.schema)(av)

          assert(actual)(isRight(equalTo(CaseClassOfNonEmptySet(NonEmptySet("tag1", "tag2")))))
        },
        test("decodes NonEmptySet of String with single element") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("tags") -> AttributeValue.StringSet(Set("only-tag"))
            )
          )

          val actual = Codec.decoder(CaseClassOfNonEmptySet.schema)(av)

          assert(actual)(isRight(equalTo(CaseClassOfNonEmptySet(NonEmptySet("only-tag")))))
        },
        test("decodes NonEmptySet of Int from native NumberSet") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("nums") -> AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2)))
            )
          )

          val actual = Codec.decoder(CaseClassOfNonEmptySetOfInt.schema)(av)

          assert(actual)(isRight(equalTo(CaseClassOfNonEmptySetOfInt(NonEmptySet(1, 2)))))
        },
        test("decodes NonEmptySet of Int with single element") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("nums") -> AttributeValue.NumberSet(Set(BigDecimal(42)))
            )
          )

          val actual = Codec.decoder(CaseClassOfNonEmptySetOfInt.schema)(av)

          assert(actual)(isRight(equalTo(CaseClassOfNonEmptySetOfInt(NonEmptySet(42)))))
        },
        test("decodes NonEmptySet using Item API") {
          val item = Item("tags" -> Set("tag1", "tag2"))

          val actual = DynamoDBQuery.fromItem[CaseClassOfNonEmptySet](item)

          assert(actual)(isRight(equalTo(CaseClassOfNonEmptySet(NonEmptySet("tag1", "tag2")))))
        }
      )
    )

}
