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
import zio.dynamodb.blocks.ddbexpr.dsl._
// Only needed here to reference the ADT node types (DdbKeyExpr.Extended, DdbExpr.And) in
// isSubtype[...] assertions below — real call sites never need this, only tests that
// inspect the internal expression shape do. put/get/scan/=== etc. all come from `dsl._`
// alone, unqualified, since dsl extends DdbExprApiSyntax/DdbKeyExprSyntax/DdbExprSyntax directly.
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbKeyExpr }
import zio.test._
import zio.test.Assertion._

/**
 * Every other spec in this module imports `DdbExprApi._`, `DdbKeyExpr._`, and a selective
 *  subset of `DdbExpr` members separately, with a comment explaining why: `DdbExpr` and
 *  `DdbKeyExpr` each used to declare their own `derivedCodec`, so a plain `DdbExpr._` wildcard
 *  alongside `DdbKeyExpr._` was an ambiguous-implicit error. This spec exercises the single
 *  `import zio.dynamodb.blocks.ddbexpr.dsl._` facade across CRUD, key expressions, condition
 *  combinators (`&&`), and update syntax in one file — the exact combination that used to
 *  require three-to-four import lines and the workaround comment — to prove the facade
 *  actually replaces all of it, not just the trivial single-predicate case. Note `put`/`get`/
 *  `scan` etc. are callable unqualified (no `DdbExprApi.` prefix needed) since `dsl` mixes
 *  their defining trait in directly.
 */
object DslSpec extends ZIOSpecDefault {

  // ── Models ───────────────────────────────────────────────────────────────────

  private sealed trait Priority
  private object Priority {
    case object Low  extends Priority
    case object High extends Priority
    implicit val schema: Schema[Priority] = Schema.derived
  }

  private case class Task(id: String, score: Int, priority: Priority)
  private object Task extends CompanionOptics[Task] {
    implicit val schema: Schema[Task]  = Schema.derived
    val id: Lens[Task, String]         = $(_.id)
    val score: Lens[Task, Int]         = $(_.score)
    val priority: Lens[Task, Priority] = $(_.priority)
  }

  private def run[A](q: DynamoDBQuery[_, A]): A = DummyIOInterpreter.run(q).unsafeRun()

  // Mirrors the docs/index.md "See it in action" snippet exactly (model names included) as a
  // compile-time check of that precise shape — a method's body is type-checked whether or not
  // it's ever called, so this fails the build if the doc's code stops compiling, without
  // needing a real AwsInterpreter[zio.Task] (schema-ddbexpr doesn't depend on any interpreter
  // module) to actually run it.
  private sealed trait Genre
  private object Genre {
    case object Drama  extends Genre
    case object Comedy extends Genre
    implicit val schema: Schema[Genre] = Schema.derived
  }

  private final case class Movie(id: String, genre: Genre)
  private object Movie extends CompanionOptics[Movie] {
    implicit val schema: Schema[Movie] = Schema.derived
    val id: Lens[Movie, String]        = $(_.id)
    val genre: Lens[Movie, Genre]      = $(_.genre)
  }

  private def docsIndexExample(interpreter: AwsInterpreter[zio.Task], table: String) =
    for {
      _     <- interpreter.run(put("movies", Movie("m1", Genre.Drama)))
      movie <- interpreter.run(get[Movie]("movies")(Movie.id.partitionKey === "m1"))
      page  <- interpreter.run(scan[Movie]("movies", 20).filter(Movie.genre === Genre.Drama))
    } yield (movie, page)

  def spec = suite("dsl facade")(
    test("put/get CRUD, callable unqualified via dsl") {
      val putQuery = put("tasks", Task("t1", 42, Priority.High))
      val getQuery = get[Task]("tasks")(Task.id.partitionKey === "t1")
      assertTrue(run(putQuery).isEmpty && run(getQuery).isLeft)
    },
    test("partitionKey === plus sortKey > builds an Extended key expression (DdbKeyExpr syntax)") {
      val keyExpr = Task.id.partitionKey === "t1" && Task.score.sortKey > 10
      assert(keyExpr)(isSubtype[DdbKeyExpr.Extended[_, _]](anything))
    },
    // SchemaExpr && SchemaExpr resolves via ZB's own native &&, producing a SchemaExpr —
    // it's mixing a SchemaExpr predicate with a DDB-specific one (.attributeExists) that
    // actually needs DdbExpr's SchemaExprBoolBridge to combine them into a DdbExpr.
    test("&& combinator mixing a SchemaExpr predicate with a DDB-specific one (SchemaExprBoolBridge)") {
      val cond = Task.score > 0 && Task.id.attributeExists
      assert(cond)(isSubtype[DdbExpr.And[_]](anything))
    },
    test(".filter with a combined condition runs through scan") {
      val scanQuery = scan[Task]("tasks", 20).filter(Task.score > 0 && Task.priority === Priority.High)
      assertTrue(run(scanQuery).items.isEmpty)
    },
    test("update syntax (OpticUpdateOps) renders a SET action") {
      val action = Task.score.set(99)
      assertTrue(action.render.execute._2.startsWith("set"))
    }
  )
}
