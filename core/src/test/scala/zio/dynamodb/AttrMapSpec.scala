/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.test._
import zio.test.Assertion.{ equalTo, hasField, isSome, isSubtype }

object AttrMapSpec extends ZIOSpecDefault {

  def spec = suite("AttrMap")(
    toAttributeValueSuite,
    getSuite,
    getOptionSuite,
    getItemSuite,
    getOptionalItemSuite,
    getIterableItemSuite,
    getOptionalIterableItemSuite,
    addSuite,
    jMapViewSuite,
    avMapJMapViewSuite
  )

  private val toAttributeValueSuite = suite("toAttributeValue")(
    test("converts AttrMap to AttributeValue.Map") {
      val m = Item("id" -> "1")
      assert(m.toAttributeValue)(
        isSubtype[AttributeValue.Map](
          hasField[AttributeValue.Map, Option[AttributeValue]](
            "value",
            _.value.get(AttributeValue.String("id")),
            isSome(equalTo(AttributeValue.String("1")))
          )
        )
      )
    }
  )

  private val getSuite = suite("get")(
    test("returns Right when field exists") {
      val m = Item("id" -> "abc")
      assertTrue(m.get[String]("id") == Right("abc"))
    },
    test("returns Left DecodingError when field not found") {
      val m = Item("id" -> "abc")
      assertTrue(m.get[String]("missing").isLeft)
    },
    test("returns Left when wrong type") {
      val m = Item("id" -> "abc")
      assertTrue(m.get[Int]("id").isLeft)
    }
  )

  private val getOptionSuite = suite("getOption")(
    test("returns Some(value) when field exists") {
      val m = Item("flag" -> true)
      assertTrue(m.getOption[Boolean]("flag").contains(true))
    },
    test("returns None when field missing") {
      val m = Item("id" -> "abc")
      assertTrue(m.getOption[String]("missing").isEmpty)
    }
  )

  private val getItemSuite = suite("getItem")(
    test("decodes nested item via function") {
      val inner = Item("name" -> "bob")
      val outer = Item("inner" -> inner)
      assertTrue(outer.getItem[String]("inner")(_.get[String]("name")) == Right("bob"))
    },
    test("returns Left when nested field missing") {
      val inner = Item("x" -> "y")
      val outer = Item("inner" -> inner)
      assertTrue(outer.getItem[String]("inner")(_.get[String]("missing")).isLeft)
    }
  )

  private val getOptionalItemSuite = suite("getOptionalItem")(
    test("returns Right(Some(...)) when field present and decode succeeds") {
      val inner = Item("n" -> 42)
      val outer = Item("inner" -> inner)
      assertTrue(outer.getOptionalItem[Int]("inner")(_.get[Int]("n")) == Right(Some(42)))
    },
    test("returns Right(None) when field absent") {
      val m = Item("id" -> "1")
      assertTrue(m.getOptionalItem[Int]("missing")(_.get[Int]("n")) == Right(None))
    }
  )

  private val getIterableItemSuite = suite("getIterableItem")(
    test("returns Left when field not found") {
      val outer  = Item("id" -> "1")
      val result = outer.getIterableItem[Int]("missing")(_.get[Int]("n"))
      assertTrue(result.isLeft)
    }
  )

  private val getOptionalIterableItemSuite = suite("getOptionalIterableItem")(
    test("returns Right(None) when field absent") {
      val m = Item("id" -> "1")
      assertTrue(m.getOptionalIterableItem[Int]("missing")(_.get[Int]("n")) == Right(None))
    }
  )

  private val addSuite = suite("+")(
    test("adds a new field to the map") {
      val m  = Item("id" -> "1")
      val m2 = m + ("name" -> AttributeValue.String("alice"))
      assertTrue(m2.map.get("name").contains(AttributeValue.String("alice")))
    }
  )

  private val jMapViewSuite = suite("JMapView")(
    test("fromJavaMap wraps a java HashMap without copying") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      jm.put("k", AttributeValue.String("v"))
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(m.map.get("k").contains(AttributeValue.String("v")))
    },
    test("JMapView.get returns Some for existing key") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      jm.put("x", AttributeValue.Bool(true))
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(m.map.get("x").contains(AttributeValue.Bool(true)))
    },
    test("JMapView.get returns None for missing key") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(m.map.get("nope").isEmpty)
    },
    test("JMapView.size returns correct size") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      jm.put("a", AttributeValue.Null)
      jm.put("b", AttributeValue.Null)
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(m.map.size == 2)
    },
    test("JMapView.isEmpty is false for non-empty map") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      jm.put("k", AttributeValue.Null)
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(!m.map.isEmpty)
    },
    test("JMapView.isEmpty is true for empty map") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(m.map.isEmpty)
    },
    test("JMapView.contains returns true for existing key") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      jm.put("id", AttributeValue.String("1"))
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(m.map.contains("id"))
    },
    test("JMapView.contains returns false for missing key") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      val m  = AttrMap.fromJavaMap(jm)
      assertTrue(!m.map.contains("missing"))
    },
    test("JMapView.iterator iterates all entries") {
      val jm    = new java.util.HashMap[String, AttributeValue]()
      jm.put("a", AttributeValue.String("1"))
      jm.put("b", AttributeValue.String("2"))
      val m     = AttrMap.fromJavaMap(jm)
      val pairs = m.map.iterator.toList
      assertTrue(pairs.size == 2)
    },
    test("JMapView.updated with AttributeValue returns new map with added key") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      jm.put("a", AttributeValue.String("1"))
      val m  = AttrMap.fromJavaMap(jm)
      val m2 = m.map.updated("b", AttributeValue.String("2"))
      assertTrue(m2.get("b").contains(AttributeValue.String("2")) && m.map.get("b").isEmpty)
    },
    test("JMapView.removed returns new map without the key") {
      val jm = new java.util.HashMap[String, AttributeValue]()
      jm.put("a", AttributeValue.String("1"))
      jm.put("b", AttributeValue.String("2"))
      val m  = AttrMap.fromJavaMap(jm)
      val m2 = m.map.removed("a")
      assertTrue(m2.get("a").isEmpty && m2.get("b").contains(AttributeValue.String("2")))
    },
    test("JMapView.hash.builder accumulates entries and produces correct map") {
      val builder = JMapView.hash.builder[String, AttributeValue]
      builder.addOne("k1", AttributeValue.String("v1"))
      builder.addOne("k2", AttributeValue.Number(BigDecimal(2)))
      val map     = builder.result
      assertTrue(
        map.get("k1").contains(AttributeValue.String("v1")) &&
          map.get("k2").contains(AttributeValue.Number(BigDecimal(2)))
      )
    },
    test("JMapView.hash.builder.++= adds multiple entries") {
      val builder = JMapView.hash.builder[String, AttributeValue]
      builder ++= List("x" -> AttributeValue.String("a"), "y" -> AttributeValue.Bool(false))
      val map     = builder.result
      assertTrue(map.get("x").contains(AttributeValue.String("a")))
    },
    test("JMapView.hash.builder.clear empties the builder") {
      val builder = JMapView.hash.builder[String, AttributeValue]
      builder.addOne("k", AttributeValue.Null)
      builder.clear()
      val map     = builder.result
      assertTrue(map.isEmpty)
    },
    test("JMapView.hash.single creates a single-entry map") {
      val map = JMapView.hash.single("k", AttributeValue.String("v"))
      assertTrue(map.get("k").contains(AttributeValue.String("v")) && map.size == 1)
    },
    test("JMapView.linked.builder accumulates entries") {
      val builder = JMapView.linked.builder[String, AttributeValue]
      builder.addOne("k1", AttributeValue.String("v1"))
      val map     = builder.result
      assertTrue(map.get("k1").contains(AttributeValue.String("v1")))
    },
    test("JMapView.linked.single creates a single-entry map") {
      val map = JMapView.linked.single("k", AttributeValue.Number(BigDecimal(1)))
      assertTrue(map.get("k").contains(AttributeValue.Number(BigDecimal(1))) && map.size == 1)
    },
    test("JMapView.hash.builder clone function invoked on updated") {
      val builder = JMapView.hash.builder[String, AttributeValue]
      builder.addOne("a", AttributeValue.String("1"))
      val map     = builder.result
      val map2    = map.updated("b", AttributeValue.String("2"))
      assertTrue(map2.get("b").contains(AttributeValue.String("2")))
    },
    test("JMapView.linked.builder clone function invoked on updated") {
      val builder = JMapView.linked.builder[String, AttributeValue]
      builder.addOne("a", AttributeValue.String("1"))
      val map     = builder.result
      val map2    = map.updated("b", AttributeValue.String("2"))
      assertTrue(map2.get("b").contains(AttributeValue.String("2")))
    },
    test("JMapView.linked.single clone function invoked on updated") {
      val map  = JMapView.linked.single("k", AttributeValue.String("v"))
      val map2 = map.updated("k2", AttributeValue.String("v2"))
      assertTrue(map2.get("k2").contains(AttributeValue.String("v2")))
    },
    test("foreachEntry iterates via JMapView path") {
      val jm    = new java.util.HashMap[String, AttributeValue]()
      jm.put("a", AttributeValue.String("1"))
      val m     = AttrMap.fromJavaMap(jm)
      var found = false
      m.foreachEntry { (k, _) => if (k == "a") found = true }
      assertTrue(found)
    },
    test("foreachEntry iterates via regular Scala map path") {
      val m     = AttrMap(Map("a" -> AttributeValue.String("1")))
      var found = false
      m.foreachEntry { (k, _) => if (k == "a") found = true }
      assertTrue(found)
    }
  )

  private val avMapJMapViewSuite = suite("AttributeValue.Map.JMapView")(
    test("hash.builder addOne stores AttributeValue.String keys") {
      val map = JMapView.hash
        .builder[AttributeValue.String, AttributeValue]
        .addOne(AttributeValue.String("k"), AttributeValue.String("v"))
        .result
      assertTrue(map.get(AttributeValue.String("k")).contains(AttributeValue.String("v")))
    },
    test("hash.single creates single-entry map with AttributeValue.String key") {
      val map = JMapView.hash.single(AttributeValue.String("k"), AttributeValue.Number(BigDecimal(1)))
      assertTrue(
        map.get(AttributeValue.String("k")).contains(AttributeValue.Number(BigDecimal(1))) &&
          map.size == 1
      )
    },
    test("linked.builder addOne stores AttributeValue.String keys") {
      val map = JMapView.linked
        .builder[AttributeValue.String, AttributeValue]
        .addOne(AttributeValue.String("x"), AttributeValue.Bool(true))
        .result
      assertTrue(map.get(AttributeValue.String("x")).contains(AttributeValue.Bool(true)))
    },
    test("linked.single creates single-entry map with AttributeValue.String key") {
      val map = JMapView.linked.single(AttributeValue.String("y"), AttributeValue.Null)
      assertTrue(map.get(AttributeValue.String("y")).contains(AttributeValue.Null))
    },
    test("getNullable returns value for existing key") {
      val map = JMapView.hash
        .builder[AttributeValue.String, AttributeValue]
        .addOne(AttributeValue.String("k"), AttributeValue.String("v"))
        .result
      val jmv = map.asInstanceOf[JMapView[AttributeValue.String, AttributeValue]]
      assertTrue(jmv.getNullable(AttributeValue.String("k")) == AttributeValue.String("v"))
    },
    test("getNullable returns null for missing key") {
      val map = JMapView.hash.builder[AttributeValue.String, AttributeValue].result
      val jmv = map.asInstanceOf[JMapView[AttributeValue.String, AttributeValue]]
      assertTrue(jmv.getNullable(AttributeValue.String("missing")) == null)
    },
    test("updated with AttributeValue value produces new map with added entry") {
      val map  = JMapView.hash
        .builder[AttributeValue.String, AttributeValue]
        .addOne(AttributeValue.String("a"), AttributeValue.String("1"))
        .result
      val map2 = map.updated(AttributeValue.String("b"), AttributeValue.String("2"))
      assertTrue(
        map2.get(AttributeValue.String("b")).contains(AttributeValue.String("2")) &&
          map.get(AttributeValue.String("b")).isEmpty
      )
    },
    test("removed returns new map without the key") {
      val map  = JMapView.hash
        .builder[AttributeValue.String, AttributeValue]
        .addOne(AttributeValue.String("a"), AttributeValue.String("1"))
        .addOne(AttributeValue.String("b"), AttributeValue.String("2"))
        .result
      val map2 = map.removed(AttributeValue.String("a"))
      assertTrue(
        map2.get(AttributeValue.String("a")).isEmpty &&
          map2.get(AttributeValue.String("b")).contains(AttributeValue.String("2"))
      )
    },
    test("builder.++= adds multiple entries") {
      val builder = JMapView.hash.builder[AttributeValue.String, AttributeValue]
      builder ++= List(
        AttributeValue.String("x") -> AttributeValue.Bool(true),
        AttributeValue.String("y") -> AttributeValue.Null
      )
      val map     = builder.result
      assertTrue(
        map.get(AttributeValue.String("x")).contains(AttributeValue.Bool(true)) &&
          map.get(AttributeValue.String("y")).contains(AttributeValue.Null)
      )
    },
    test("builder.clear empties the builder") {
      val builder = JMapView.hash.builder[AttributeValue.String, AttributeValue]
      builder.addOne(AttributeValue.String("k"), AttributeValue.Null)
      builder.clear()
      assertTrue(builder.result.isEmpty)
    }
  )
}
