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

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExprApi, DdbKeyExpr }
// Import OpticUpdateOps from DdbExpr selectively to avoid dual-derivedCodec ambiguity
// (both DdbExpr._ and DdbKeyExpr._ expose derivedCodec with the same signature).
import zio.dynamodb.blocks.ddbexpr.DdbExpr.OpticUpdateOps
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.test._
import zio.test.Assertion._

object DdbExprUpdateSpec extends ZIOSpecDefault {

  // ── Models ───────────────────────────────────────────────────────────────────

  private case class Record(id: String, score: Int, count: Int, tags: List[String], labels: Set[String])
  private object Record extends CompanionOptics[Record] {
    implicit val schema: Schema[Record]   = Schema.derived
    val id: Lens[Record, String]          = $(_.id)
    val score: Lens[Record, Int]          = $(_.score)
    val count: Lens[Record, Int]          = $(_.count)
    val tags: Lens[Record, List[String]]  = $(_.tags)
    val labels: Lens[Record, Set[String]] = $(_.labels)
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def render(action: UpdateExpression.Action[_]): String = action.render.execute._2

  private def run[A](q: DynamoDBQuery[_, A]): A = DummyIOInterpreter.run(q).unsafeRun()

  // ── Spec ─────────────────────────────────────────────────────────────────────

  def spec = suite("DdbExpr — update expressions")(
    suite("set(value)")(
      test("renders SET with field alias and value alias") {
        val action = Record.score.set(42)
        assert(render(action))(startsWithString("set") && containsString("=") && containsString(":v"))
      }
    ),

    suite("set(otherOptic) — field copy")(
      test("renders SET path = other_path with two field aliases and no value alias") {
        val action   = Record.count.set(Record.score)
        val rendered = render(action)
        // both fields get aliases; no bare value alias (:v) since the RHS is a path
        assert(rendered)(startsWithString("set") && containsString("="))
      }
    ),

    suite("setIfNotExists(value)")(
      test("renders SET path = if_not_exists(path, :v)") {
        val action = Record.score.setIfNotExists(0)
        assert(render(action))(containsString("if_not_exists") && containsString(":v"))
      }
    ),

    suite("remove")(
      test("remove renders REMOVE field_alias") {
        val action = Record.score.remove
        assert(render(action))(startsWithString("remove"))
      },
      test("remove(index) renders REMOVE field_alias[index]") {
        val action = Record.tags.remove(0)
        assert(render(action))(startsWithString("remove") && containsString("[0]"))
      }
    ),

    suite("add(value)")(
      test("renders ADD field_alias :v") {
        val action = Record.count.add(1)
        assert(render(action))(startsWithString("add") && containsString(":v"))
      }
    ),

    suite("addSet(value)")(
      test("renders ADD field_alias :v on a set attribute") {
        val action = Record.labels.addSet(Set("new-label"))
        assert(render(action))(startsWithString("add") && containsString(":v"))
      }
    ),

    suite("deleteFromSet(value)")(
      test("renders DELETE field_alias :v on a set attribute") {
        val action = Record.labels.deleteFromSet(Set("bad-label"))
        assert(render(action))(startsWithString("delete") && containsString(":v"))
      }
    ),

    suite("appendList / prependList")(
      test("appendList renders SET path = list_append(path, :v)") {
        val action = Record.tags.appendList(Seq("new-tag"))
        assert(render(action))(containsString("list_append") && containsString(":v"))
      },
      test("prependList renders SET path = list_append(:v, path) — items prepended") {
        val action = Record.tags.prependList(Seq("first-tag"))
        assert(render(action))(containsString("list_append") && containsString(":v"))
      }
    ),

    suite("increment / decrement (arithmetic SET operand)")(
      test("increment renders SET path = path + :v") {
        val action   = Record.count.increment(5)
        val rendered = render(action)
        assert(rendered)(startsWithString("set") && containsString("+") && containsString(":v"))
      },
      test("decrement renders SET path = path - :v") {
        val action   = Record.count.decrement(1)
        val rendered = render(action)
        assert(rendered)(startsWithString("set") && containsString("-") && containsString(":v"))
      },
      test("increment: field alias appears on both sides of assignment (path operand reuses alias)") {
        val action   = Record.count.increment(5)
        val rendered = render(action)
        // format is: "set #alias = #alias + :v"
        // strip "set " and split on " = " to get lhs and rhs
        val body     = rendered.stripPrefix("set").trim
        val sides    = body.split(" = ", 2)
        assertTrue(sides.length == 2) &&
        assert(sides(1))(startsWithString(sides(0)))
      }
    ),
    suite("action composition with +")(
      test("set + remove renders both SET and REMOVE clauses") {
        val action = Record.score.set(42) + Record.tags.remove
        assert(render(action))(containsString("set") && containsString("remove"))
      },
      test("set + set renders single SET clause covering both paths") {
        val action = Record.score.set(42) + Record.count.set(0)
        assert(render(action))(startsWithString("set"))
      },
      test("three-action composition renders correctly") {
        val action   = Record.score.set(42) + Record.count.add(1) + Record.tags.remove
        val rendered = render(action)
        assert(rendered)(containsString("set") && containsString("add") && containsString("remove"))
      }
    ),

    suite("DdbExprApi.update")(
      test("update with set action runs without error") {
        val q = DdbExprApi
          .update[Record]("records")(Record.id.partitionKey === "r1")(Record.score.set(99))
        assertTrue(run(q).isEmpty)
      },
      test("update with composed set + add runs without error") {
        val q = DdbExprApi
          .update[Record]("records")(Record.id.partitionKey === "r1")(Record.score.set(99) + Record.count.add(1))
        assertTrue(run(q).isEmpty)
      }
      // Range expressions (sortKey > / between / beginsWith) on update are now a compile-time error:
      // DdbExprApi.update takes DdbKeyExpr.PrimaryKey, which Extended does not satisfy.
    ),
    suite("Action.Failure")(
      test("Action.Failure raises DecodingError on updateItem run") {
        val action = UpdateExpression.Action.Failure[Record]("bad action")
        val q      = DynamoDBQuery.updateItem("records", PrimaryKey("id" -> "r1"))(action)
        val thrown = scala.util.Try(run(q)).failed.toOption
        assertTrue(thrown.exists(_.isInstanceOf[DynamoDBError.ItemError.DecodingError]))
      },
      test("Action.Failure message is preserved in the thrown error") {
        val action = UpdateExpression.Action.Failure[Record]("specific action failure")
        val q      = DynamoDBQuery.updateItem("records", PrimaryKey("id" -> "r1"))(action)
        val thrown = scala.util.Try(run(q)).failed.toOption
        assertTrue(thrown.exists(_.getMessage.contains("specific action failure")))
      }
    )
  )
}
