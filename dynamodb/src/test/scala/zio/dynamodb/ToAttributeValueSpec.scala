package zio.dynamodb

import zio.Chunk
import zio.test.{ DefaultRunnableSpec, _ }

//import scala.Predef.{ ScalaMap => Map }

object Foo {
  val primaryKey         = PrimaryKey("field1" -> 1.0, "field2" -> "X", "field3" -> true)
  val itemSimple         = Item("field1" -> 1, "field2" -> "X", "field3" -> true)
  val itemNested         = Item("field1" -> 1, "field2" -> "X", "field3" -> Item("field4" -> 1, "field5" -> "X"))
  val itemEvenMoreNested = Item(
    "field1" -> 1,
    "field2" -> "X",
    "field3" -> Item("field4" -> 1, "field5" -> Item("field6" -> 1, "field7" -> "X"))
  )
}

/*
  private[dynamodb] final case class Binary(value: Iterable[Byte])                extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                         extends AttributeValue
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]])   extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])        extends AttributeValue
  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  private[dynamodb] final case class Number(value: BigDecimal)                    extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  private[dynamodb] object Null                                                   extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)                   extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString])
 */

object ToAttributeValueSpec extends DefaultRunnableSpec {
  val ScalaMap = scala.collection.immutable.Map

  override def spec =
    suite("AttrMap suite")(
      test("AttrMap.empty.map equals empty map") {
        val attrMap = AttrMap.empty
        assert(attrMap.map)(Assertion.equalTo(ScalaMap.empty[String, AttributeValue]))
      },
      test("AttrMap of a binary field values equals a Map of AttributeValue.Binary") {
        val attrMap = AttrMap(("f1", List(Byte.MinValue)))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Binary(Chunk(Byte.MinValue)))))
      },
      test("AttrMap of a Boolean field values equals a Map of AttributeValue.Bool") {
        val attrMap = AttrMap(("f1", true))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Bool(true))))
      },
      test("AttrMap of a Set of binary field values equals a Map of AttributeValue.BinarySet") {
        val attrMap = AttrMap(("f1", Set(Chunk(Byte.MinValue))))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.BinarySet(Set(Chunk(Byte.MinValue))))))
      },
      test("AttrMap of a list field values equals an AttributeValue.List") {
        val attrMap = AttrMap(("f1", Chunk("s")))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.List(Chunk(AttributeValue.String("s"))))))
      },
      test("AttrMap of a nested AttrMap numeric field values equals an AttributeValue.Map") {
        val attrMap = AttrMap(("f1", AttrMap("f2" -> 1)))
        assert(attrMap.map)(
          Assertion.equalTo(
            ScalaMap(
              "f1" -> AttributeValue.Map(
                ScalaMap(AttributeValue.String("f2") -> AttributeValue.Number(BigDecimal(1.0)))
              )
            )
          )
        )
      },
      // TODO: expand to all numeric types
      test("AttrMap of a number field values equals an AttributeValue.Number") {
        val attrMap = AttrMap("f1" -> 1)
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Number(BigDecimal(1.0)))))
      },
      test("AttrMap of a number set field values equals an AttributeValue.NumberSet") {
        val attrMap = AttrMap("f1" -> Set(1))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.NumberSet(Set(BigDecimal(1.0))))))
      },
      /*
TODO: fix ambiguous implicit values:
 both value attrMapToAttributeValue in object ToAttributeValue of type zio.dynamodb.ToAttributeValue[zio.dynamodb.AttrMap]
 and value stringToAttributeValue in object ToAttributeValue of type zio.dynamodb.ToAttributeValue[String]
 match expected type zio.dynamodb.ToAttributeValue[Null]
        val attrMap = Item("f1" -> null)

       */
//      test("AttrMap of a null field values equals a Map of AttributeValue.Null") {
//        val attrMap = Item("f1" -> null)
//        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Null)))
//      },
      test("AttrMap of a string field values equals a Map of AttributeValue.String") {
        val attrMap = Item("f1" -> "s")
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.String("s"))))
      },
      test("AttrMap of a string set field values equals a Map of AttributeValue.StringSet") {
        val attrMap = Item("f1" -> Set("s"))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.StringSet(Set("s")))))
      }
    )
}
