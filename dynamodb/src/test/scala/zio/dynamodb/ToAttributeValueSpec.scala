package zio.dynamodb

import zio.Chunk
import zio.test.{ DefaultRunnableSpec, _ }

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
      test("AttrMap of an Int field values equals an AttributeValue.Number") {
        val attrMap = AttrMap("f1" -> 1)
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Number(BigDecimal(1.0)))))
      },
      test("AttrMap of a Long field values equals an AttributeValue.Number") {
        val attrMap = AttrMap("f1" -> 1L)
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Number(BigDecimal(1.0)))))
      },
      test("AttrMap of a Double field values equals an AttributeValue.Number") {
        val attrMap = AttrMap("f1" -> 1.0d)
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Number(BigDecimal(1.0)))))
      },
      test("AttrMap of a Float field values equals an AttributeValue.Number") {
        val attrMap = AttrMap("f1" -> 1.0f)
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Number(BigDecimal(1.0)))))
      },
      test("AttrMap of a BigDecimal field values equals an AttributeValue.Number") {
        val attrMap = AttrMap("f1" -> BigDecimal(1.0))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Number(BigDecimal(1.0)))))
      },
      test("AttrMap of a number set field values equals an AttributeValue.NumberSet") {
        val attrMap = AttrMap("f1" -> Set(1))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.NumberSet(Set(BigDecimal(1.0))))))
      },
      test("AttrMap of a null field values equals a Map of AttributeValue.Null") {
        val attrMap = Item("f1" -> AttributeValue.Null())
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.Null())))
      },
      test("AttrMap of a String field values equals a Map of AttributeValue.String") {
        val attrMap = Item("f1" -> "s")
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.String("s"))))
      },
      test("AttrMap of a String Set field values equals a Map of AttributeValue.StringSet") {
        val attrMap = Item("f1" -> Set("s"))
        assert(attrMap.map)(Assertion.equalTo(ScalaMap("f1" -> AttributeValue.StringSet(Set("s")))))
      }
    )
}
