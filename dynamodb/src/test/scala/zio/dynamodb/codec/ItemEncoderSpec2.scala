package zio.dynamodb.codec

import zio.dynamodb._
import zio.test.Assertion._
import zio.test._

import zio.test.ZIOSpecDefault
import zio.schema.DeriveSchema
import zio.schema.annotation.noDiscriminator

object ItemEncoderSpec2 extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemEncoder2 Suite")(mainSuite)

  //@discriminatorName("fooBar")
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

  val mainSuite = suite("Main Suite")(
    test("encodes Two") {
      val expectedItem: Item =
        Item(
          Map(
            "s" -> AttributeValue.String("1")
          )
        )

      val item = DynamoDBQuery.toItem[MixedSimpleEnum](MixedSimpleEnum.Two("1"))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes One") {
      val expectedItem: Item =
        Item(
          Map(
            "i" -> AttributeValue.Number(1)
          )
        )

      val item = DynamoDBQuery.toItem[MixedSimpleEnum](MixedSimpleEnum.One(1))

      assert(item)(equalTo(expectedItem))
    }
  )

}
