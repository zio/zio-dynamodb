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

import zio.dynamodb.ProjectionExpression._
import zio.dynamodb.ProjectionExpression.$
import zio.test._

object ProjectionExpressionSpec extends ZIOSpecDefault {

  def spec = suite("ProjectionExpression")(
    toStringSuite,
    compositionSuite
  )

  // ---------------------------------------------------------------------------
  // toString
  // ---------------------------------------------------------------------------

  private val toStringSuite = suite("toString")(
    test("Root renders as empty string") {
      assertTrue(Root.toString == "")
    },
    test("single MapElement") {
      assertTrue(MapElement(Root, "foo").toString == "foo")
    },
    test("two nested MapElements") {
      assertTrue(MapElement(MapElement(Root, "a"), "b").toString == "a.b")
    },
    test("three nested MapElements") {
      assertTrue(MapElement(MapElement(MapElement(Root, "a"), "b"), "c").toString == "a.b.c")
    },
    test("ListElement index 0") {
      assertTrue(ListElement(MapElement(Root, "items"), 0).toString == "items[0]")
    },
    test("ListElement index 5") {
      assertTrue(ListElement(MapElement(Root, "items"), 5).toString == "items[5]")
    },
    test("MapElement after ListElement") {
      assertTrue(MapElement(ListElement(MapElement(Root, "items"), 2), "name").toString == "items[2].name")
    },
    test("multi-dimensional list index") {
      assertTrue(ListElement(ListElement(MapElement(Root, "m"), 1), 2).toString == "m[1][2]")
    }
  )

  // ---------------------------------------------------------------------------
  // >>> and apply conveniences
  // ---------------------------------------------------------------------------

  private val compositionSuite = suite(">>> (path composition)")(
    test("composing with Root is identity") {
      val pe = MapElement(Root, "foo")
      assertTrue((pe >>> Root).toString == pe.toString)
    },
    test("composing two map paths") {
      assertTrue(($("a.b") >>> MapElement(Root, "c")).toString == "a.b.c")
    },
    test("composing a map path with a list path") {
      assertTrue(($("items") >>> ListElement(Root, 0)).toString == "items[0]")
    },
    test("triple composition") {
      assertTrue(($("a") >>> $("b") >>> $("c")).toString == "a.b.c")
    },
    test("apply(index) convenience appends list element") {
      assertTrue($("items")(0).toString == "items[0]")
    },
    test("apply(key) convenience appends map element") {
      assertTrue($("outer")("inner").toString == "outer.inner")
    },
    test("chained apply calls") {
      assertTrue($("a")("b")(0).toString == "a.b[0]")
    }
  )
}
