package zio.dynamodb

import zio.dynamodb.ProjectionExpression.$
import zio.test._

object ProjectionExpressionSyntaxSpec extends ZIOSpecDefault {

  private def renderUpdate(a: UpdateExpression.Action[_]): String = {
    val (_, s) = UpdateExpression(a).render.execute
    s
  }

  private def expr(ce: ConditionExpression[_]): String = {
    val (_, s) = ce.render.execute
    s
  }

  private val agePE   = $("age")
  private val namePE  = $("name")
  private val countPE = $("count")
  private val tagsPE  = $("tags")
  private val itemsPE = $("items")

  def spec = suite("ProjectionExpressionSyntax")(
    updateActionsSuite,
    conditionsSuite
  )

  // ---------------------------------------------------------------------------
  // Update actions
  // Standalone SetAction.render inserts operand before path, so for set(value)
  // the value alias (:v0) always precedes the target path alias (#n1).
  // ---------------------------------------------------------------------------

  private val updateActionsSuite = suite("update actions")(
    suite("set")(
      test("set(value) — value alias precedes path alias") {
        assertTrue(renderUpdate(agePE.set(42)) == "set #n1 = :v0")
      },
      test("set(pe) — source path alias precedes target path alias") {
        // PathOperand(count) → #n0; then target age → #n1
        assertTrue(renderUpdate(agePE.set(countPE)) == "set #n1 = #n0")
      }
    ),
    suite("setIfNotExists")(
      test("setIfNotExists(value) — self path reused for both check and target") {
        // IfNotExists: pe(score)→#n0, value→:v1; target reuses #n0
        val scorePE = $("score")
        assertTrue(renderUpdate(scorePE.setIfNotExists(0)) == "set #n0 = if_not_exists(#n0, :v1)")
      },
      test("setIfNotExists(pe, value) — fallback path distinct from target path") {
        // IfNotExists: pe(default)→#n0, value→:v1; target(total)→#n2
        val totalPE   = $("total")
        val defaultPE = $("default")
        assertTrue(renderUpdate(totalPE.setIfNotExists(defaultPE, 0)) == "set #n2 = if_not_exists(#n0, :v1)")
      }
    ),
    suite("append / prepend")(
      test("append(a) — list_append with self as first argument") {
        assertTrue(renderUpdate(itemsPE.append("x")) == "set #n0 = list_append(#n0, :v1)")
      },
      test("appendList(xs) — list_append with self as first argument") {
        assertTrue(renderUpdate(itemsPE.appendList(List("x", "y"))) == "set #n0 = list_append(#n0, :v1)")
      },
      test("prepend(a) — list_append with list as first argument for prepend semantics") {
        assertTrue(renderUpdate(itemsPE.prepend("x")) == "set #n0 = list_append(:v1, #n0)")
      },
      test("prependList(xs) — list_append with list as first argument for prepend semantics") {
        assertTrue(renderUpdate(itemsPE.prependList(List("x", "y"))) == "set #n0 = list_append(:v1, #n0)")
      }
    ),
    suite("add")(
      test("add(value) — path alias precedes value alias") {
        assertTrue(renderUpdate(countPE.add(5)) == "add #n0 :v1")
      },
      test("addSet(set) — path alias precedes set value alias") {
        assertTrue(renderUpdate(tagsPE.addSet(Set("new"))) == "add #n0 :v1")
      }
    ),
    suite("deleteFromSet")(
      test("deleteFromSet — path alias precedes set value alias") {
        assertTrue(renderUpdate(tagsPE.deleteFromSet(Set("old"))) == "delete #n0 :v1")
      }
    )
  )

  // ---------------------------------------------------------------------------
  // Condition expressions
  // For comparators: left operand (path) is inserted first → #n0; right (value) → :v1.
  // For contains: value is inserted first → :v0; path → #n1.
  // For between: path → #n0; min → :v1; max → :v2.
  // ---------------------------------------------------------------------------

  private val conditionsSuite = suite("condition expressions")(
    suite("between")(
      test("between — path alias precedes both bound aliases") {
        assertTrue(expr(agePE.between(18, 65)) == "#n0 BETWEEN :v1 AND :v2")
      },
      test("between — same min and max reuses one value alias") {
        assertTrue(expr(agePE.between(42, 42)) == "#n0 BETWEEN :v1 AND :v1")
      }
    ),
    suite("in / inSet")(
      test("in(single value) — renders IN with one value alias") {
        assertTrue(expr(agePE.in(42)) == "#n0 IN (:v1)")
      },
      test("inSet(singleton) — renders IN with one value alias") {
        assertTrue(expr(agePE.inSet(Set(42))) == "#n0 IN (:v1)")
      },
      test("in(multiple) — expression starts with IN keyword and contains all aliases") {
        val e = expr(agePE.in(1, 2, 3))
        assertTrue(e.startsWith("#n0 IN (") && e.contains(":v1") && e.contains(":v2") && e.contains(":v3"))
      }
    ),
    suite("contains / containsSet")(
      test("contains — value alias precedes path alias") {
        assertTrue(expr(namePE.contains("foo")) == "contains(#n1, :v0)")
      },
      test("containsSet — chained AND with shared path alias") {
        // contains("foo") && contains("bar"): :v0, #n1 for first; :v2, #n1(reused) for second
        assertTrue(expr(namePE.containsSet("foo", Set("bar"))) == "(contains(#n1, :v0)) AND (contains(#n1, :v2))")
      },
      test("containsSet with empty tail — equivalent to single contains") {
        assertTrue(expr(namePE.containsSet("foo", Set.empty)) == "contains(#n1, :v0)")
      }
    ),
    suite("comparators")(
      suite("value operand")(
        test("=== renders =") {
          assertTrue(expr(namePE === "alice") == "(#n0) = (:v1)")
        },
        test("<> renders <>") {
          assertTrue(expr(namePE <> "alice") == "(#n0) <> (:v1)")
        },
        test("< renders <") {
          assertTrue(expr(agePE < 18) == "(#n0) < (:v1)")
        },
        test("<= renders <=") {
          assertTrue(expr(agePE <= 18) == "(#n0) <= (:v1)")
        },
        test("> renders >") {
          assertTrue(expr(agePE > 18) == "(#n0) > (:v1)")
        },
        test(">= renders >=") {
          assertTrue(expr(agePE >= 18) == "(#n0) >= (:v1)")
        }
      ),
      suite("PE operand")(
        test("=== renders =") {
          assertTrue(expr(namePE === countPE) == "(#n0) = (#n1)")
        },
        test("<> renders <>") {
          assertTrue(expr(namePE <> countPE) == "(#n0) <> (#n1)")
        },
        test("< renders <") {
          assertTrue(expr(agePE < countPE) == "(#n0) < (#n1)")
        },
        test("<= renders <=") {
          assertTrue(expr(agePE <= countPE) == "(#n0) <= (#n1)")
        },
        test("> renders >") {
          assertTrue(expr(agePE > countPE) == "(#n0) > (#n1)")
        },
        test(">= renders >=") {
          assertTrue(expr(agePE >= countPE) == "(#n0) >= (#n1)")
        }
      )
    )
  )
}
