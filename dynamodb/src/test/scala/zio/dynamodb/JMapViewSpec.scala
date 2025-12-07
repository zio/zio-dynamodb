package zio.dynamodb

import zio.test.{ assertTrue, ZIOSpecDefault }

object JMapViewSpec extends ZIOSpecDefault {
  val hashMap: Map[AttributeValue.String, AttributeValue] = {
    val hashMapBuilder = AttributeValue.Map.JMapView.hash.builder
    hashMapBuilder.addOne("key1", AttributeValue.String("value1"))
    hashMapBuilder.addOne("key2", AttributeValue.String("value2"))
    hashMapBuilder.result
  }

  val linkedHashMap: Map[AttributeValue.String, AttributeValue] = {
    val hashMapBuilder = AttributeValue.Map.JMapView.linked.builder
    hashMapBuilder.addOne("key1", AttributeValue.String("value1"))
    hashMapBuilder.addOne("key2", AttributeValue.String("value2"))
    hashMapBuilder.result
  }

  val singleHashMap = AttributeValue.Map.JMapView.hash.single(
    "singleKey",
    AttributeValue.String("singleValue")
  )

  val singleLinkedHashMap = AttributeValue.Map.JMapView.linked.single(
    "singleKey",
    AttributeValue.String("singleValue")
  )

  override val spec = suite("JMapViewSpec")(
    suite("builds an immutable Scala Map using an underlying Java HashMap")(
      test("get") {
        assertTrue(
          hashMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("value1")),
          hashMap.get(AttributeValue.String("key2")) == Some(AttributeValue.String("value2"))
        )
      },
      test("iterator") {
        val entries = hashMap.iterator.toList
        assertTrue(
          entries.contains((AttributeValue.String("key1"), AttributeValue.String("value1"))),
          entries.contains((AttributeValue.String("key2"), AttributeValue.String("value2")))
        )
      },
      test("updated") {
        val updatedMap = hashMap.updated(AttributeValue.String("key1"), AttributeValue.String("newValue1"))
        assertTrue(
          updatedMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("newValue1")),
          hashMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("value1"))
        )
      },
      test("removed") {
        val removedMap = hashMap.removed(AttributeValue.String("key1"))
        assertTrue(
          removedMap.get(AttributeValue.String("key1")) == None,
          hashMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("value1"))
        )
      },
      test("single") {
        assertTrue(
          singleHashMap.size == 1,
          singleHashMap.get(AttributeValue.String("singleKey")) == Some(AttributeValue.String("singleValue"))
        )
      }
    ),
    suite("builds an immutable Scala Map using an underlying Java LinkedHashMap")(
      test("get") {
        assertTrue(
          linkedHashMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("value1")),
          linkedHashMap.get(AttributeValue.String("key2")) == Some(AttributeValue.String("value2"))
        )
      },
      test("iterator") {
        val entries = linkedHashMap.iterator.toList
        assertTrue(
          entries.contains((AttributeValue.String("key1"), AttributeValue.String("value1"))),
          entries.contains((AttributeValue.String("key2"), AttributeValue.String("value2")))
        )
      },
      test("updated") {
        val updatedMap = linkedHashMap.updated(AttributeValue.String("key1"), AttributeValue.String("newValue1"))
        assertTrue(
          updatedMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("newValue1")),
          linkedHashMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("value1"))
        )
      },
      test("removed") {
        val removedMap = linkedHashMap.removed(AttributeValue.String("key1"))
        assertTrue(
          removedMap.get(AttributeValue.String("key1")) == None,
          linkedHashMap.get(AttributeValue.String("key1")) == Some(AttributeValue.String("value1"))
        )
      },
      test("single") {
        assertTrue(
          singleLinkedHashMap.size == 1,
          singleLinkedHashMap.get(AttributeValue.String("singleKey")) == Some(AttributeValue.String("singleValue"))
        )
      }
    )
  )
}
