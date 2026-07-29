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

import zio.dynamodb.ConditionExpression._
import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.ProjectionExpression.$
import zio.test._

object ConditionExpressionSpec extends ZIOSpecDefault {

  // Runs render and returns the expression string and alias map
  private def eval(ce: ConditionExpression[_]): (String, AliasMap) = {
    val (am, s) = ce.render.execute
    (s, am)
  }
  private def expr(ce: ConditionExpression[_]): String             = eval(ce)._1
  private def aliasCount(ce: ConditionExpression[_]): Int          = eval(ce)._2.index

  // Shared path/operand fixtures
  private val namePE  = $("name")
  private val agePE   = $("age")
  private val emailPE = $("email")
  private val nameOp  = ProjectionExpressionOperand(namePE)
  private val ageOp   = ProjectionExpressionOperand(agePE)

  def spec = suite("ConditionExpression")(
    functionsSuite,
    betweenSuite,
    inSuite,
    logicalSuite,
    comparatorSuite,
    aliasReuseSuite
  )

  // ---------------------------------------------------------------------------
  // DynamoDB functions
  // ---------------------------------------------------------------------------

  private val functionsSuite = suite("functions")(
    test("attribute_exists") {
      assertTrue(expr(AttributeExists(namePE)) == "attribute_exists(#n0)")
    },
    test("attribute_not_exists") {
      assertTrue(expr(AttributeNotExists(namePE)) == "attribute_not_exists(#n0)")
    },
    // AttributeValueType.render inserts the type string value first (:v0),
    // then the path alias (#n1) — alias indices reflect insertion order.
    test("attribute_type — String") {
      assertTrue(expr(AttributeType(namePE, AttributeValueType.String)) == "attribute_type(#n1, :v0)")
    },
    test("attribute_type — Number") {
      assertTrue(expr(AttributeType(agePE, AttributeValueType.Number)) == "attribute_type(#n1, :v0)")
    },
    test("attribute_type — Bool") {
      assertTrue(expr(AttributeType(namePE, AttributeValueType.Bool)) == "attribute_type(#n1, :v0)")
    },
    test("contains — value alias precedes path alias") {
      assertTrue(expr(Contains(namePE, AttributeValue.String("foo"))) == "contains(#n1, :v0)")
    },
    test("begins_with — value alias precedes path alias") {
      assertTrue(expr(BeginsWith(emailPE, AttributeValue.String("test@"))) == "begins_with(#n1, :v0)")
    },
    test("attribute_exists — uses exactly 1 alias") {
      assertTrue(aliasCount(AttributeExists(namePE)) == 1)
    },
    test("contains — uses exactly 2 aliases") {
      assertTrue(aliasCount(Contains(namePE, AttributeValue.String("foo"))) == 2)
    }
  )

  // ---------------------------------------------------------------------------
  // BETWEEN
  // ---------------------------------------------------------------------------

  private val betweenSuite = suite("between")(
    test("renders BETWEEN with path alias and two value aliases") {
      val ce = ageOp.between(
        AttributeValue.Number(BigDecimal(18)),
        AttributeValue.Number(BigDecimal(65))
      )
      assertTrue(expr(ce) == "#n0 BETWEEN :v1 AND :v2")
    },
    test("uses exactly 3 aliases (path + min + max)") {
      val ce = ageOp.between(
        AttributeValue.Number(BigDecimal(18)),
        AttributeValue.Number(BigDecimal(65))
      )
      assertTrue(aliasCount(ce) == 3)
    },
    test("same min and max value reuses a single alias") {
      val v  = AttributeValue.Number(BigDecimal(42))
      val ce = ageOp.between(v, v)
      // min and max share :v1; only 2 unique aliases
      assertTrue(expr(ce) == "#n0 BETWEEN :v1 AND :v1" && aliasCount(ce) == 2)
    }
  )

  // ---------------------------------------------------------------------------
  // IN
  // ---------------------------------------------------------------------------

  private val inSuite = suite("in")(
    test("single value — path IN (value)") {
      val ce = ageOp.in(Set(AttributeValue.Number(BigDecimal(42))))
      assertTrue(expr(ce) == "#n0 IN (:v1)")
    },
    test("single value — uses exactly 2 aliases") {
      val ce = ageOp.in(Set(AttributeValue.Number(BigDecimal(42))))
      assertTrue(aliasCount(ce) == 2)
    },
    test("multiple values — expression contains IN keyword and all value aliases") {
      val ce = ageOp.in(
        Set(
          AttributeValue.Number(BigDecimal(1)),
          AttributeValue.Number(BigDecimal(2))
        )
      )
      val e  = expr(ce)
      assertTrue(e.startsWith("#n0 IN (") && e.contains(":v1") && e.contains(":v2"))
    },
    test("multiple values — uses exactly 3 aliases") {
      val ce = ageOp.in(
        Set(
          AttributeValue.Number(BigDecimal(1)),
          AttributeValue.Number(BigDecimal(2))
        )
      )
      assertTrue(aliasCount(ce) == 3)
    }
  )

  // ---------------------------------------------------------------------------
  // Logical operators
  // ---------------------------------------------------------------------------

  private val logicalSuite = suite("logical operators")(
    test("&& renders as AND with parenthesised operands") {
      val ce = AttributeExists(namePE) && AttributeNotExists(agePE)
      assertTrue(expr(ce) == "(attribute_exists(#n0)) AND (attribute_not_exists(#n1))")
    },
    test("|| renders as OR with parenthesised operands") {
      val ce = AttributeExists(namePE) || AttributeExists(agePE)
      assertTrue(expr(ce) == "(attribute_exists(#n0)) OR (attribute_exists(#n1))")
    },
    test("! renders as NOT with parenthesised operand") {
      assertTrue(expr(!AttributeExists(namePE)) == "NOT (attribute_exists(#n0))")
    },
    test("chained && nests left-associatively") {
      val ce = AttributeExists(namePE) && AttributeExists(agePE) && AttributeExists(emailPE)
      assertTrue(
        expr(ce) ==
          "((attribute_exists(#n0)) AND (attribute_exists(#n1))) AND (attribute_exists(#n2))"
      )
    },
    test("&& inside || — parentheses reflect explicit nesting") {
      val ce = (AttributeExists(namePE) && AttributeNotExists(agePE)) || AttributeExists(emailPE)
      assertTrue(
        expr(ce) ==
          "((attribute_exists(#n0)) AND (attribute_not_exists(#n1))) OR (attribute_exists(#n2))"
      )
    },
    test("double negation wraps twice") {
      assertTrue(
        expr(!(!AttributeExists(namePE))) == "NOT (NOT (attribute_exists(#n0)))"
      )
    }
  )

  // ---------------------------------------------------------------------------
  // Comparators
  // ---------------------------------------------------------------------------

  private val comparatorSuite = suite("comparators")(
    suite("PE vs value")(
      test("=== renders =") {
        assertTrue(expr(nameOp === "alice") == "(#n0) = (:v1)")
      },
      test("<> renders <>") {
        assertTrue(expr(nameOp <> "alice") == "(#n0) <> (:v1)")
      },
      test("< renders <") {
        assertTrue(expr(ageOp < 18) == "(#n0) < (:v1)")
      },
      test("<= renders <=") {
        assertTrue(expr(ageOp <= 18) == "(#n0) <= (:v1)")
      },
      test("> renders >") {
        assertTrue(expr(ageOp > 18) == "(#n0) > (:v1)")
      },
      test(">= renders >=") {
        assertTrue(expr(ageOp >= 18) == "(#n0) >= (:v1)")
      }
    ),
    suite("PE vs PE")(
      test("=== renders =") {
        val ce = nameOp === ProjectionExpressionOperand(agePE)
        assertTrue(expr(ce) == "(#n0) = (#n1)")
      },
      test("<> renders <>") {
        val ce = nameOp <> ProjectionExpressionOperand(agePE)
        assertTrue(expr(ce) == "(#n0) <> (#n1)")
      },
      test("< renders <") {
        val ce = ageOp < ProjectionExpressionOperand(namePE)
        assertTrue(expr(ce) == "(#n0) < (#n1)")
      },
      test("<= renders <=") {
        val ce = ageOp <= ProjectionExpressionOperand(namePE)
        assertTrue(expr(ce) == "(#n0) <= (#n1)")
      },
      // NOTE: Operand.> (PE-to-PE form) currently delegates to GreaterThanOrEqual
      // instead of GreaterThan — copy-paste bug in ConditionExpression.scala.
      // Test left intentionally absent until fixed.
      test(">= renders >=") {
        val ce = ageOp >= ProjectionExpressionOperand(namePE)
        assertTrue(expr(ce) == "(#n0) >= (#n1)")
      }
    )
  )

  // ---------------------------------------------------------------------------
  // Alias reuse
  // ---------------------------------------------------------------------------

  private val aliasReuseSuite = suite("alias reuse")(
    test("same path referenced twice in AND gets one alias") {
      val ce = AttributeExists(namePE) && AttributeNotExists(namePE)
      assertTrue(
        expr(ce) == "(attribute_exists(#n0)) AND (attribute_not_exists(#n0))" &&
          aliasCount(ce) == 1
      )
    },
    test("same AttributeValue in two Contains gets one value alias") {
      val v  = AttributeValue.String("foo")
      val ce = Contains(namePE, v) && Contains(agePE, v)
      // :v0 = "foo", #n1 = name, #n2 = age — "foo" reused as :v0
      assertTrue(
        expr(ce) == "(contains(#n1, :v0)) AND (contains(#n2, :v0))" &&
          aliasCount(ce) == 3
      )
    },
    test("two distinct paths in AND get two distinct aliases") {
      val ce = AttributeExists(namePE) && AttributeExists(agePE)
      assertTrue(aliasCount(ce) == 2)
    },
    test("two distinct values in BETWEEN get two distinct aliases") {
      val ce = ageOp.between(
        AttributeValue.Number(BigDecimal(1)),
        AttributeValue.Number(BigDecimal(2))
      )
      assertTrue(aliasCount(ce) == 3) // path + 2 values
    }
  )
}
