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
import zio.dynamodb.blocks.ddbexpr.{ DdbKeyExpr, DdbKeyExprInterpreter }
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.test._
import zio.test.Assertion._

object DdbKeyExprSpec extends ZIOSpecDefault {

  // ── Models ───────────────────────────────────────────────────────────────────

  private case class Task(id: String, score: Int, name: String)
  private object Task extends CompanionOptics[Task] {
    implicit val schema: Schema[Task] = Schema.derived
    val id: Lens[Task, String]        = $(_.id)
    val score: Lens[Task, Int]        = $(_.score)
    val name: Lens[Task, String]      = $(_.name)
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def render(kce: KeyConditionExpr[_]): String = kce.render.execute._2

  private def interpret[S](expr: DdbKeyExpr[S]): Either[String, KeyConditionExpr[S]] =
    DdbKeyExprInterpreter.toKeyConditionExpr(expr)

  // ── Spec ─────────────────────────────────────────────────────────────────────

  def spec = suite("DdbKeyExpr")(
    suite("partition key only")(
      test("partitionKey produces PartitionKeyEquals") {
        val expr = Task.id.partitionKey === "alice"
        assert(interpret(expr))(isRight(isSubtype[KeyConditionExpr.PartitionKeyEquals[Task]](anything)))
      },
      test("partitionKey renders field alias = value alias") {
        val expr = Task.id.partitionKey === "alice"
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("#n") && containsString("=") && containsString(":v"))
          )
      }
    ),
    suite("composite (partitionKey + sortKey equality)")(
      test("partitionKey && sortKey === produces CompositePrimaryKeyExpr") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey === 42
        assert(interpret(expr))(isRight(isSubtype[KeyConditionExpr.CompositePrimaryKeyExpr[Task]](anything)))
      },
      test("partitionKey && sortKey === renders partitionKey AND sortKey") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey === 42
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("AND") && containsString("=") && containsString(":v"))
          )
      }
    ),
    suite("extended (partitionKey + sortKey range / function)")(
      test("partitionKey && sortKey > produces ExtendedCompositePrimaryKeyExpr") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey > 10
        assert(interpret(expr))(isRight(isSubtype[KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[Task]](anything)))
      },
      test("partitionKey && sortKey > renders >") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey > 10
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">"))
          )
      },
      test("partitionKey && sortKey >= renders >=") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey >= 10
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString(">="))
          )
      },
      test("partitionKey && sortKey < renders <") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey < 100
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("<"))
          )
      },
      test("partitionKey && sortKey <= renders <=") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey <= 100
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("<="))
          )
      },
      test("partitionKey && sortKey.between renders BETWEEN ... AND ...") {
        val expr = Task.id.partitionKey === "alice" && Task.score.sortKey.between(10, 100)
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("BETWEEN") && containsString("AND") && containsString(":v"))
          )
      },
      test("partitionKey && sortKey.beginsWith renders begins_with") {
        val expr = Task.id.partitionKey === "alice" && Task.name.sortKey.beginsWith("prefix")
        interpret(expr)
          .map(render)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(containsString("begins_with") && containsString(":v"))
          )
      }
    )
  )

  private def assertNever(msg: String) = assertTrue(msg == "impossible")
}
