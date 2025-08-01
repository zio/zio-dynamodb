package zio.dynamodb.blocks

import zio.dynamodb.{ AttrMap }
import zio.blocks.schema._
import zio.test._

object AbstractFieldAntiPatternSpec extends ZIOSpecDefault {

  // see if abstract field anti pattern is possible to implement
  @Modifier.config(
    "discriminatorName",
    "antiPatternType"
  ) // TODO: see if we can add modifier programmatically to derived schema
  sealed trait AbstractFieldAntiPattern {
    def id: String
  }
  object AbstractFieldAntiPattern       {
    // direct concrete implementation, no intermediate abstract classes
    final case class AbstractFieldAntiPattern1(id: String, field1: String) extends AbstractFieldAntiPattern
    object AbstractFieldAntiPattern1                                       extends CompanionOptics[AbstractFieldAntiPattern1] {
      implicit val schema: Schema[AbstractFieldAntiPattern1] = Schema.derived
      val id: Lens[AbstractFieldAntiPattern1, String]        = optic(_.id)
      val field1: Lens[AbstractFieldAntiPattern1, String]    = optic(_.field1)
    }
    // direct concrete implementation, no intermediate abstract classes
    final case class AbstractFieldAntiPattern2(id: String, field2: String) extends AbstractFieldAntiPattern
    object AbstractFieldAntiPattern2                                       extends CompanionOptics[AbstractFieldAntiPattern2] {
      implicit val schema: Schema[AbstractFieldAntiPattern2] = Schema.derived
      val id: Lens[AbstractFieldAntiPattern2, String]        = optic(_.id)
      val field2: Lens[AbstractFieldAntiPattern2, String]    = optic(_.field2)
    }
    implicit val schema: Schema[AbstractFieldAntiPattern] = Schema.derived
  }

  // TODO: looks like nested abstract fields are not supported in ZIO Blocks
  // sealed trait NestedAbstractFieldAntiPattern {
  //   def id: String
  // }
  // object NestedAbstractFieldAntiPattern       {
  //   final case class AbstractFieldAntiPattern1(id: String, field1: String) extends NestedAbstractFieldAntiPattern
  //   object AbstractFieldAntiPattern1                                       extends CompanionOptics[AbstractFieldAntiPattern1] {
  //     implicit val schema: Schema[AbstractFieldAntiPattern1] = Schema.derived
  //     val id: Lens[AbstractFieldAntiPattern1, String]        = optic(_.id)
  //     val field1: Lens[AbstractFieldAntiPattern1, String]    = optic(_.field1)
  //   }
  //   sealed trait Nested extends NestedAbstractFieldAntiPattern {
  //     def id: String
  //     def foo: String
  //   }
  //   object Nested extends CompanionOptics[Nested] {
  //     final case class Nested1(id: String, foo: String) extends Nested {
  //       implicit val schema: Schema[Nested1] = Schema.derived
  //       val id: Lens[Nested1, String]        = optic(_.id)
  //       val foo: Lens[Nested1, String]       = optic(_.foo)
  //     }
  //     implicit val schema: Schema[Nested] = Schema.derived
  //   }

  //   implicit val schema: Schema[NestedAbstractFieldAntiPattern] = Schema.derived
  // }

  val spec = suite("abstract field anti pattern")(
    test("encode top level sum type") {
      val p: AbstractFieldAntiPattern = AbstractFieldAntiPattern.AbstractFieldAntiPattern1("1", "field1")
      val enc                         = BlocksCodec.encoder[AbstractFieldAntiPattern]
      val expected                    = AttrMap(
        "id"              -> "1",
        "field1"          -> "field1",
        "antiPatternType" -> "AbstractFieldAntiPattern1"
      ).toAttributeValue
      assertTrue(enc(p) == expected)
    },
    test("decode top level sum type") {
      val av  = AttrMap(
        "id"              -> "1",
        "field1"          -> "field1",
        "antiPatternType" -> "AbstractFieldAntiPattern1"
      ).toAttributeValue
      val dec = BlocksCodec.decoder[AbstractFieldAntiPattern]
      assertTrue(dec(av) == Right(AbstractFieldAntiPattern.AbstractFieldAntiPattern1("1", "field1")))
    }
  )

}
