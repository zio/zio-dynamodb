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

import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.Action._
import zio.dynamodb.UpdateExpression.SetOperand._
import zio.test._

object UpdateExpressionSpec extends ZIOSpecDefault {

  private def eval(ue: UpdateExpression[_]): (String, AliasMap) = {
    val (am, s) = ue.render.execute
    (s, am)
  }
  private def expr(ue: UpdateExpression[_]): String             = eval(ue)._1
  private def aliasCount(ue: UpdateExpression[_]): Int          = eval(ue)._2.index

  private val agePE   = $("age")
  private val namePE  = $("name")
  private val countPE = $("count")
  private val tagsPE  = $("tags")

  def spec = suite("UpdateExpression")(
    singleActionSuite,
    setOperandSuite,
    combinedActionsSuite,
    aliasReuseSuite
  )

  // ---------------------------------------------------------------------------
  // Single actions — standalone render (not combined via +)
  // ---------------------------------------------------------------------------

  private val singleActionSuite = suite("single actions")(
    // Standalone SetAction.render inserts operand first, then path.
    test("SET value — value alias precedes path alias") {
      val ue = UpdateExpression(SetAction(agePE, ValueOperand(AttributeValue.Number(42))))
      assertTrue(expr(ue) == "set #n1 = :v0")
    },
    test("SET value — uses exactly 2 aliases") {
      val ue = UpdateExpression(SetAction(agePE, ValueOperand(AttributeValue.Number(42))))
      assertTrue(aliasCount(ue) == 2)
    },
    test("SET path operand — source path alias precedes target path alias") {
      // PathOperand(name) inserted first → #n0; then target age → #n1
      val ue = UpdateExpression(SetAction(agePE, PathOperand(namePE)))
      assertTrue(expr(ue) == "set #n1 = #n0")
    },
    test("REMOVE — renders remove with single path alias") {
      assertTrue(expr(UpdateExpression(RemoveAction(namePE))) == "remove #n0")
    },
    test("REMOVE — uses exactly 1 alias") {
      assertTrue(aliasCount(UpdateExpression(RemoveAction(namePE))) == 1)
    },
    test("ADD — path alias precedes value alias") {
      val ue = UpdateExpression(AddAction(countPE, AttributeValue.Number(5)))
      assertTrue(expr(ue) == "add #n0 :v1")
    },
    test("ADD — uses exactly 2 aliases") {
      assertTrue(aliasCount(UpdateExpression(AddAction(countPE, AttributeValue.Number(5)))) == 2)
    },
    test("DELETE — path alias precedes value alias") {
      val ue = UpdateExpression(DeleteAction(tagsPE, AttributeValue.StringSet(Set("old"))))
      assertTrue(expr(ue) == "delete #n0 :v1")
    }
  )

  // ---------------------------------------------------------------------------
  // SetOperand variants
  // ---------------------------------------------------------------------------

  private val setOperandSuite = suite("SetOperand")(
    test("Plus — left operand alias precedes right operand alias") {
      // Plus.render: left first → :v0, right → :v1; then path → #n2
      val ue = UpdateExpression(
        SetAction(agePE, Plus(ValueOperand(AttributeValue.Number(10)), ValueOperand(AttributeValue.Number(5))))
      )
      assertTrue(expr(ue) == "set #n2 = :v0 + :v1")
    },
    test("Minus — path operand minus value operand") {
      // PathOperand(age) → #n0, value → :v1; path age reuses #n0
      val ue = UpdateExpression(SetAction(agePE, Minus(PathOperand(agePE), ValueOperand(AttributeValue.Number(1)))))
      assertTrue(expr(ue) == "set #n0 = #n0 - :v1" && aliasCount(ue) == 2)
    },
    test("list_append — appends list to existing attribute") {
      val itemsPE = $("items")
      val list    = AttributeValue.List(Iterable(AttributeValue.String("x")))
      // ListAppend: pe → #n0, list → :v1; path reuses #n0
      val ue      = UpdateExpression(SetAction(itemsPE, ListAppend(itemsPE, list)))
      assertTrue(expr(ue) == "set #n0 = list_append(#n0, :v1)")
    },
    test("list_append (prepend) — list argument comes first for prepend semantics") {
      val itemsPE = $("items")
      val list    = AttributeValue.List(Iterable(AttributeValue.String("x")))
      // ListPrepend: pe → #n0, list → :v1; output: list_append($list, $pe)
      val ue      = UpdateExpression(SetAction(itemsPE, ListPrepend(itemsPE, list)))
      assertTrue(expr(ue) == "set #n0 = list_append(:v1, #n0)")
    },
    test("if_not_exists — uses path alias for both check and default") {
      val scorePE = $("score")
      // IfNotExists: pe → #n0, value → :v1; path reuses #n0
      val ue      = UpdateExpression(SetAction(scorePE, IfNotExists(scorePE, AttributeValue.Number(0))))
      assertTrue(expr(ue) == "set #n0 = if_not_exists(#n0, :v1)")
    }
  )

  // ---------------------------------------------------------------------------
  // Combined actions (via +)
  // ---------------------------------------------------------------------------

  private val combinedActionsSuite = suite("combined actions")(
    // Combined actions use miniRender where path is inserted FIRST (unlike standalone render).
    test("SET + REMOVE — path alias precedes value alias in combined mode") {
      val ue = UpdateExpression(SetAction(agePE, ValueOperand(AttributeValue.Number(42))) + RemoveAction(namePE))
      // miniRender: age→#n0, 42→:v1; then remove name→#n2
      assertTrue(expr(ue) == "set #n0 = :v1 remove #n2")
    },
    test("two SET actions — comma-separated within set clause") {
      val ue = UpdateExpression(
        SetAction(agePE, ValueOperand(AttributeValue.Number(42))) +
          SetAction(namePE, ValueOperand(AttributeValue.String("alice")))
      )
      // age→#n0, 42→:v1; name→#n2, "alice"→:v3
      assertTrue(expr(ue) == "set #n0 = :v1,#n2 = :v3")
    },
    test("REMOVE + SET — SET keyword always rendered before REMOVE regardless of construction order") {
      val ue = UpdateExpression(RemoveAction(namePE) + SetAction(agePE, ValueOperand(AttributeValue.Number(42))))
      // Actions.render always processes SET before REMOVE in for-comprehension
      assertTrue(expr(ue) == "set #n0 = :v1 remove #n2")
    },
    test("all four action types — rendered in SET REMOVE ADD DELETE order") {
      val aPE = $("a"); val bPE = $("b"); val cPE = $("c"); val dPE = $("d")
      val ue  = UpdateExpression(
        SetAction(aPE, ValueOperand(AttributeValue.Number(1))) +
          RemoveAction(bPE) +
          AddAction(cPE, AttributeValue.Number(5)) +
          DeleteAction(dPE, AttributeValue.StringSet(Set("x")))
      )
      assertTrue(expr(ue) == "set #n0 = :v1 remove #n2 add #n3 :v4 delete #n5 :v6")
    },
    test("all four action types — uses exactly 7 aliases") {
      val aPE = $("a"); val bPE = $("b"); val cPE = $("c"); val dPE = $("d")
      val ue  = UpdateExpression(
        SetAction(aPE, ValueOperand(AttributeValue.Number(1))) +
          RemoveAction(bPE) +
          AddAction(cPE, AttributeValue.Number(5)) +
          DeleteAction(dPE, AttributeValue.StringSet(Set("x")))
      )
      assertTrue(aliasCount(ue) == 7)
    }
  )

  // ---------------------------------------------------------------------------
  // Alias reuse
  // ---------------------------------------------------------------------------

  private val aliasReuseSuite = suite("alias reuse")(
    test("same path in two REMOVE actions reuses one alias") {
      val ue = UpdateExpression(RemoveAction(namePE) + RemoveAction(namePE))
      assertTrue(expr(ue) == "remove #n0,#n0" && aliasCount(ue) == 1)
    },
    test("same value in two SET actions reuses one value alias") {
      val v  = AttributeValue.Number(42)
      val ue = UpdateExpression(
        SetAction(agePE, ValueOperand(v)) +
          SetAction(namePE, ValueOperand(v))
      )
      // age→#n0, 42→:v1; name→#n2, 42→:v1(reused)
      assertTrue(
        expr(ue) == "set #n0 = :v1,#n2 = :v1" &&
          aliasCount(ue) == 3
      )
    },
    test("path used as both SET target and PathOperand shares one alias") {
      // PathOperand(age) → #n0; value → :v1; SET target age reuses #n0
      val ue = UpdateExpression(SetAction(agePE, Minus(PathOperand(agePE), ValueOperand(AttributeValue.Number(1)))))
      assertTrue(aliasCount(ue) == 2)
    },
    test("distinct paths in combined actions get distinct aliases") {
      val ue = UpdateExpression(RemoveAction(agePE) + RemoveAction(namePE))
      assertTrue(aliasCount(ue) == 2)
    }
  )
}
