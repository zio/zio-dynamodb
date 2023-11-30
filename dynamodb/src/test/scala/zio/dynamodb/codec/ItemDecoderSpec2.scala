package zio.dynamodb.codec

import zio.dynamodb._
import zio.test.Assertion._
import zio.test.{ ZIOSpecDefault, _ }
import zio.schema.annotation.noDiscriminator
import zio.schema.DeriveSchema

object ItemDecoderSpec2 extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemDecoder Suite")(mainSuite)

  @noDiscriminator
  sealed trait MixedSimpleEnum
  object MixedSimpleEnum {
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
    }
  )

}
