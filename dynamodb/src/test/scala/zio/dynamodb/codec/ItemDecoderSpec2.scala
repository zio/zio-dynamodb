package zio.dynamodb.codec

import zio.dynamodb._
import zio.test.Assertion._
import zio.test.{ ZIOSpecDefault, _ }
import zio.schema.annotation.noDiscriminator
import zio.schema.DeriveSchema
import zio.schema.annotation.simpleEnum

object ItemDecoderSpec2 extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemDecoder Suite")(mainSuite)

  @noDiscriminator
  sealed trait MixedSimpleEnum
  object MixedSimpleEnum {
    @simpleEnum
    case object MinusOne         extends MixedSimpleEnum
    @simpleEnum
    case object Zero             extends MixedSimpleEnum
    final case class One(i: Int) extends MixedSimpleEnum
    object One {
      implicit val schema = DeriveSchema.gen[One]
    }
    final case class Two(s: String) extends MixedSimpleEnum
    object Two {
      implicit val schema = DeriveSchema.gen[Two]
    }

    implicit val schema = DeriveSchema.gen[MixedSimpleEnum]
  }
  final case class Box(sumType: MixedSimpleEnum)
  object Box             {
    implicit val schema = DeriveSchema.gen[Box]
  }

  val mainSuite = suite("Decoder Suite")(
    test("decodes One") {
      val item: Item =
        Item(
          Map(
            "i" -> AttributeValue.Number(42)
          )
        )

      val actual = DynamoDBQuery.fromItem[MixedSimpleEnum](item)

      assert(actual)(isRight(equalTo(MixedSimpleEnum.One(i = 42))))
    },
    test("decodes Two") {
      val item: Item =
        Item(
          Map(
            "s" -> AttributeValue.String("X")
          )
        )

      val actual = DynamoDBQuery.fromItem[MixedSimpleEnum](item)

      assert(actual)(isRight(equalTo(MixedSimpleEnum.Two(s = "X"))))
    },
    test("decodes Zero") {
      val item: Item = Item(Map("sumType" -> AttributeValue.String("Zero")))

      val actual = DynamoDBQuery.fromItem[Box](item)

      assert(actual)(isRight(equalTo(Box(MixedSimpleEnum.Zero))))
    }
  )

}
