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

import zio.dynamodb.KeyConditionExpr._
import zio.dynamodb.ProjectionExpression.$
import zio.test._

object KeyConditionExpressionSpec extends ZIOSpecDefault {

  private def render(expr: KeyConditionExpr[_]): String = expr.render.execute._2

  private val namePE = $("name")
  private val agePE  = $("age")

  private val pk = namePE.partitionKey
  private val sk = agePE.sortKey

  def spec = suite("KeyConditionExpression")(
    partitionKeyEqualsSuite,
    compositePrimaryKeySuite,
    extendedCompositePrimaryKeySuite,
    sortKeyExprSuite,
    asAttrMapSuite
  )

  private val partitionKeyEqualsSuite = suite("PartitionKeyEquals")(
    test("renders partition key equality") {
      val expr = pk === "alice"
      assertTrue(render(expr).contains("= :v"))
    },
    test("asAttrMap contains partition key value") {
      val expr = pk === "alice"
      assertTrue(expr.asAttrMap.map.get("name").contains(AttributeValue.String("alice")))
    },
    test("&& with SortKeyEquals builds CompositePrimaryKeyExpr") {
      val expr = pk === "alice" && sk === 30
      assertTrue(render(expr).contains("AND"))
    },
    test("&& with ExtendedSortKeyExpr builds ExtendedCompositePrimaryKeyExpr") {
      val expr = pk === "alice" && sk > 18
      assertTrue(render(expr).contains("AND"))
    }
  )

  private val compositePrimaryKeySuite = suite("CompositePrimaryKeyExpr")(
    test("renders pk AND sk equality") {
      val expr = pk === "alice" && sk === 30
      val s    = render(expr)
      assertTrue(s.contains(" AND "))
    },
    test("asAttrMap contains both keys") {
      val expr  = pk === "alice" && sk === 30
      val attrs = expr.asAttrMap
      assertTrue(
        attrs.map.get("name").contains(AttributeValue.String("alice")) &&
          attrs.map.get("age").contains(AttributeValue.Number(BigDecimal(30)))
      )
    }
  )

  private val extendedCompositePrimaryKeySuite = suite("ExtendedCompositePrimaryKeyExpr")(
    test("renders pk AND sk > value") {
      val expr = pk === "alice" && sk > 18
      val s    = render(expr)
      assertTrue(s.contains(" AND ") && s.contains(">"))
    },
    test("renders pk AND sk < value") {
      val expr = pk === "alice" && sk < 18
      val s    = render(expr)
      assertTrue(s.contains("<"))
    },
    test("renders pk AND sk >= value") {
      val expr = pk === "alice" && sk >= 18
      val s    = render(expr)
      assertTrue(s.contains(">="))
    },
    test("renders pk AND sk <= value") {
      val expr = pk === "alice" && sk <= 18
      val s    = render(expr)
      assertTrue(s.contains("<="))
    },
    test("renders pk AND sk <> value") {
      val expr = pk === "alice" && sk <> 18
      val s    = render(expr)
      assertTrue(s.contains("<>"))
    },
    test("renders pk AND sk BETWEEN min AND max") {
      val expr = pk === "alice" && sk.between(10, 20)
      val s    = render(expr)
      assertTrue(s.contains("BETWEEN"))
    },
    test("renders pk AND begins_with(sk, prefix)") {
      val expr = pk === "alice" && sk.beginsWith("pre")
      val s    = render(expr)
      assertTrue(s.contains("begins_with"))
    }
  )

  private val sortKeyExprSuite = suite("SortKeyEquals.miniRender")(
    test("renders sort key equality expression") {
      val skEq = SortKeyEquals(agePE.sortKey, AttributeValue.Number(BigDecimal(30)))
      val s    = skEq.miniRender.execute._2
      assertTrue(s.contains("=") && s.contains(":v"))
    }
  )

  private val asAttrMapSuite = suite("asAttrMap")(
    test("PartitionKeyEquals asAttrMap has one entry") {
      val expr = pk === "test"
      assertTrue(expr.asAttrMap.map.size == 1)
    },
    test("CompositePrimaryKeyExpr asAttrMap has two entries") {
      val expr = pk === "test" && sk === 5
      assertTrue(expr.asAttrMap.map.size == 2)
    }
  )
}
