package zio.dynamodb.codec

import zio.dynamodb._
import zio.test.Assertion._
import zio.test._

import zio.test.ZIOSpecDefault
import zio.schema.DeriveSchema
import zio.schema.annotation.noDiscriminator
import zio.schema.annotation.simpleEnum

object ItemEncoderSpec2 extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemEncoder2 Suite")(mainSuite)

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
    },
    test("encodes case object only enum") {
      val expectedItem: Item = Item(Map("sumType" -> AttributeValue.String("Zero")))

      val item = DynamoDBQuery.toItem(Box(MixedSimpleEnum.Zero))

      assert(item)(equalTo(expectedItem))
    }
  )

}
