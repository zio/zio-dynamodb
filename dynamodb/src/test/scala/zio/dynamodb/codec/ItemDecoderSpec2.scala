package zio.dynamodb.codec

import zio.dynamodb._
import zio.test.Assertion._
import zio.test.{ ZIOSpecDefault, _ }
import zio.schema.annotation.noDiscriminator
import zio.schema.DeriveSchema
import zio.schema.annotation.caseName
import zio.schema.annotation.fieldName

object ItemDecoderSpec2 extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemDecoder Suite")(noDiscriminatorSuite)

  @noDiscriminator
  sealed trait NoDiscriminatorEnum
  object NoDiscriminatorEnum {
    case object MinusOne                             extends NoDiscriminatorEnum
    @caseName("0")
    case object Zero                                 extends NoDiscriminatorEnum
    final case class One(@fieldName("count") i: Int) extends NoDiscriminatorEnum
    object One {
      implicit val schema = DeriveSchema.gen[One]
    }
    @caseName("2")
    final case class Two(s: String) extends NoDiscriminatorEnum
    object Two {
      implicit val schema = DeriveSchema.gen[Two]
    }

    implicit val schema = DeriveSchema.gen[NoDiscriminatorEnum]
  }
  final case class WithNoDiscriminator(sumType: NoDiscriminatorEnum)
  object WithNoDiscriminator {
    implicit val schema = DeriveSchema.gen[WithNoDiscriminator]
  }

  @noDiscriminator
  sealed trait NoDiscriminatorEnumError
  object NoDiscriminatorEnumError {
    final case class One(i: Int) extends NoDiscriminatorEnumError
    object One {
      implicit val schema = DeriveSchema.gen[One]
    }
    final case class Two(i: Int) extends NoDiscriminatorEnumError
    object Two {
      implicit val schema = DeriveSchema.gen[Two]
    }

    implicit val schema = DeriveSchema.gen[NoDiscriminatorEnumError]
  }
  final case class WithNoDiscriminatorError(sumType: NoDiscriminatorEnumError)
  object WithNoDiscriminatorError {
    implicit val schema = DeriveSchema.gen[WithNoDiscriminatorError]
  }

  val noDiscriminatorSuite = suite("@noDisriminator Decoder Suite")(
    test("decodes One case class with @fieldName") {
      val item: Item = Item("sumType" -> Item("count" -> 42))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.One(i = 42)))))
    },
    test("decodes Two case class and ignores class level @caseName (which only applies to discriminators)") {
      val item: Item = Item("sumType" -> Item("s" -> "X"))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.Two(s = "X")))))
    },
    test("decodes MinusOne case object") {
      val item: Item = Item(Map("sumType" -> AttributeValue.String("MinusOne")))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.MinusOne))))
    },
    test("decodes Zero case object with @caseName") {
      val item: Item = Item(Map("sumType" -> AttributeValue.String("0")))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.Zero))))
    },
    test("returns a Left of DecodeError when no decoder is not found") {
      val item: Item = Item("sumType" -> Item("FIELD_NOT_IN_ANY_SUBTYPE" -> "X"))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(
        isLeft(
          equalTo(
            DynamoDBError.DecodingError(message =
              "All sub type decoders failed for Map(Map(String(FIELD_NOT_IN_ANY_SUBTYPE) -> String(X)))"
            )
          )
        )
      )
    },
    test("returns a Left of DecodeError when a more than one decoder is found") {
      val item: Item = Item("sumType" -> Item("i" -> 42))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminatorError](item)

      assert(actual)(
        isLeft(
          equalTo(
            DynamoDBError.DecodingError(message =
              "More than one sub type decoder succeeded for Map(Map(String(i) -> Number(42)))"
            )
          )
        )
      )
    }
  )

}
