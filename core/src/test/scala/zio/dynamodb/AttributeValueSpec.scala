package zio.dynamodb

import zio.test._

object AttributeValueSpec extends ZIOSpecDefault {

  def spec = suite("AttributeValue")(
    showTypeSuite,
    listSuite,
    numberSetSuite,
    stringSetSuite,
    mapSuite
  )

  private val showTypeSuite = suite("showType")(
    test("Binary") {
      assertTrue(AttributeValue.Binary(List(1.toByte)).showType == "AttributeValue.Binary")
    },
    test("BinarySet") {
      assertTrue(AttributeValue.BinarySet(List(List(1.toByte))).showType == "AttributeValue.BinarySet")
    },
    test("Bool") {
      assertTrue(AttributeValue.Bool(true).showType == "AttributeValue.Bool")
    },
    test("List") {
      assertTrue(AttributeValue.List(List.empty).showType == "AttributeValue.List")
    },
    test("Map") {
      assertTrue(AttributeValue.Map(Map.empty).showType == "AttributeValue.Map")
    },
    test("Number") {
      assertTrue(AttributeValue.Number(BigDecimal(1)).showType == "AttributeValue.Number")
    },
    test("NumberSet") {
      assertTrue(AttributeValue.NumberSet(Set(BigDecimal(1))).showType == "AttributeValue.NumberSet")
    },
    test("Null") {
      assertTrue(AttributeValue.Null.showType == "AttributeValue.Null")
    },
    test("String") {
      assertTrue(AttributeValue.String("x").showType == "AttributeValue.String")
    },
    test("StringSet") {
      assertTrue(AttributeValue.StringSet(Set("a")).showType == "AttributeValue.StringSet")
    }
  )

  private val listSuite = suite("AttributeValue.List")(
    test("empty creates empty list") {
      assertTrue(AttributeValue.List.empty == AttributeValue.List(Iterable.empty))
    },
    test("+ appends a value") {
      val list    = AttributeValue.List(List(AttributeValue.String("a")))
      val updated = list + AttributeValue.String("b")
      assertTrue(updated.value.toList == List(AttributeValue.String("a"), AttributeValue.String("b")))
    }
  )

  private val numberSetSuite = suite("AttributeValue.NumberSet")(
    test("empty creates empty number set") {
      assertTrue(AttributeValue.NumberSet.empty == AttributeValue.NumberSet(Set.empty))
    },
    test("+ with valid number string adds to set") {
      val ns     = AttributeValue.NumberSet(Set(BigDecimal(1)))
      val result = ns + "2"
      assertTrue(result == Right(AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2)))))
    },
    test("+ with invalid string returns Left") {
      val ns     = AttributeValue.NumberSet.empty
      val result = ns + "not-a-number"
      assertTrue(result.isLeft)
    }
  )

  private val stringSetSuite = suite("AttributeValue.StringSet")(
    test("empty creates empty string set") {
      assertTrue(AttributeValue.StringSet.empty == AttributeValue.StringSet(Set.empty))
    },
    test("+ adds a string to the set") {
      val ss      = AttributeValue.StringSet(Set("a"))
      val updated = ss + "b"
      assertTrue(updated.value == Set("a", "b"))
    }
  )

  private val mapSuite = suite("AttributeValue.Map")(
    test("+ adds a key-value pair") {
      val m       = AttributeValue.Map(Map.empty)
      val updated = m + ("key" -> AttributeValue.String("value"))
      assertTrue(updated.value.get(AttributeValue.String("key")).contains(AttributeValue.String("value")))
    }
  )
}
