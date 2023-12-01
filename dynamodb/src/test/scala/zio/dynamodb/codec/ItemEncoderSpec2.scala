package zio.dynamodb.codec

import zio.dynamodb._
import zio.test.Assertion._
import zio.test._

import zio.test.ZIOSpecDefault
import zio.schema.DeriveSchema
import zio.schema.annotation.noDiscriminator
import zio.schema.annotation.caseName
import zio.schema.annotation.fieldName

object ItemEncoderSpec2 extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemEncoder2 Suite")(noDiscriminatorSuite)

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

  val noDiscriminatorSuite = suite("Main Suite")(
    test("encodes One case class with @fieldName") {
      val expectedItem: Item = Item("sumType" -> Item("count" -> 1))

      val item = DynamoDBQuery.toItem[WithNoDiscriminator](WithNoDiscriminator(NoDiscriminatorEnum.One(1)))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes Two case class and ignores class level @caseName (which only applies to discriminators)") {
      val expectedItem: Item = Item("sumType" -> Item("s" -> "1"))

      val item = DynamoDBQuery.toItem[WithNoDiscriminator](WithNoDiscriminator(NoDiscriminatorEnum.Two("1")))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes MinusOne case object") {
      val expectedItem: Item = Item("sumType" -> "MinusOne")

      val item = DynamoDBQuery.toItem(WithNoDiscriminator(NoDiscriminatorEnum.MinusOne))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes Zero case object with @caseName") {
      val expectedItem: Item = Item("sumType" -> "0")

      val item = DynamoDBQuery.toItem(WithNoDiscriminator(NoDiscriminatorEnum.Zero))

      assert(item)(equalTo(expectedItem))
    }
  )

}
