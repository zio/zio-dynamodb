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

import zio.dynamodb.KeyConditionExpr.{ ExtendedSortKeyExpr, SortKeyEquals }
import zio.dynamodb.ProjectionExpression._
import zio.dynamodb.ProjectionExpression.$
import zio.test._
import zio.test.Assertion.{ anything, isSubtype }

object SortKeySpec extends ZIOSpecDefault {

  private val agePE = $("age")
  private val sk    = agePE.sortKey

  // SortKeyOps (typed)
  private val typedSk: SortKey[Any, Int] = SortKey[Any, Int]("age")

  private def renderSortKeyExpr(e: ExtendedSortKeyExpr[_, _]): String =
    e.miniRender.execute._2

  def spec = suite("SortKey")(
    unknownToOpsSuite,
    typedOpsSuite
  )

  private val unknownToOpsSuite = suite("SortKeyUnknownToOps")(
    test("=== builds SortKeyEquals") {
      val expr = sk === "val"
      assert(expr)(isSubtype[SortKeyEquals[Any]](anything))
    },
    test("> builds GreaterThan") {
      val expr = sk > 10
      assertTrue(renderSortKeyExpr(expr).contains(">"))
    },
    test("< builds LessThan") {
      val expr = sk < 10
      assertTrue(renderSortKeyExpr(expr).contains("<"))
    },
    test("<> builds NotEqual") {
      val expr = sk <> 10
      assertTrue(renderSortKeyExpr(expr).contains("<>"))
    },
    test("<= builds LessThanOrEqual") {
      val expr = sk <= 10
      assertTrue(renderSortKeyExpr(expr).contains("<="))
    },
    test(">= builds GreaterThanOrEqual") {
      val expr = sk >= 10
      assertTrue(renderSortKeyExpr(expr).contains(">="))
    },
    test("between builds Between") {
      val expr = sk.between(1, 10)
      assertTrue(renderSortKeyExpr(expr).contains("BETWEEN"))
    },
    test("beginsWith builds BeginsWith") {
      val expr = sk.beginsWith("pref")
      assertTrue(renderSortKeyExpr(expr).contains("begins_with"))
    }
  )

  private val typedOpsSuite = suite("SortKeyOps (typed)")(
    test("=== builds SortKeyEquals") {
      val expr = typedSk === 5
      assert(expr)(isSubtype[SortKeyEquals[Any]](anything))
    },
    test("> builds GreaterThan") {
      val expr = typedSk > 5
      assertTrue(renderSortKeyExpr(expr).contains(">"))
    },
    test("< builds LessThan") {
      val expr = typedSk < 5
      assertTrue(renderSortKeyExpr(expr).contains("<"))
    },
    test("<> builds NotEqual") {
      val expr = typedSk <> 5
      assertTrue(renderSortKeyExpr(expr).contains("<>"))
    },
    test("<= builds LessThanOrEqual") {
      val expr = typedSk <= 5
      assertTrue(renderSortKeyExpr(expr).contains("<="))
    },
    test(">= builds GreaterThanOrEqual") {
      val expr = typedSk >= 5
      assertTrue(renderSortKeyExpr(expr).contains(">="))
    },
    test("between builds Between") {
      val expr = typedSk.between(1, 10)
      assertTrue(renderSortKeyExpr(expr).contains("BETWEEN"))
    },
    test("beginsWith builds BeginsWith") {
      val typedStrSk: SortKey[Any, String] = SortKey[Any, String]("name")
      import SortKey.SortKeyOps
      val expr                             = typedStrSk.beginsWith("pre")
      assertTrue(renderSortKeyExpr(expr).contains("begins_with"))
    }
  )
}
