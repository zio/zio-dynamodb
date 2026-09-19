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

package examples.highlevelapi

import zio.dynamodb.{ DummyIOInterpreter, Interpreter }
import zio.dynamodb.ExecuteSyntax._
import zio.test._
import zio.test.Assertion._

/**
 * `CompileTimeRejections.buildsFineFailsOnRun` builds without complaint (nothing type-gates a
 * `Map`'s key type), but running it exercises the actual failure the rest of that file only
 * describes in a comment: DynamoDB requires string map keys, so the query fails once it runs.
 *
 * The second suite below compiles each rejected snippet for real via `zio.test.typeCheck`,
 * rather than pasting a compiler error into a comment that nothing re-checks.
 */
object CompileTimeRejectionsSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("CompileTimeRejections")(
    suite("live failure")(
      test("a non-String map key builds fine but fails once the query runs") {
        val outcome = scala.util.Try(CompileTimeRejections.buildsFineFailsOnRun.execute.unsafeRun())
        assertTrue(outcome.isFailure) &&
        assert(outcome.failed.get.getMessage)(containsString("only String keys are supported in DDB"))
      }
    ),
    suite("compile-time rejections")(
      test("inSet only works on a scalar field, not a list") {
        typeCheck("""
          import zio.blocks.schema.{ CompanionOptics, Schema }
          import zio.dynamodb.blocks.ddbexpr.dsl._

          final case class Widget(id: String, tags: List[String])
          object Widget extends CompanionOptics[Widget] {
            implicit val schema: Schema[Widget] = Schema.derived
            val tags = $(_.tags)
          }

          Widget.tags.inSet(Set(List("a")))
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("remove(index) only works on a list field") {
        typeCheck("""
          import zio.blocks.schema.{ CompanionOptics, Schema }
          import zio.dynamodb.blocks.ddbexpr.dsl._

          final case class Widget(id: String, qty: Int)
          object Widget extends CompanionOptics[Widget] {
            implicit val schema: Schema[Widget] = Schema.derived
            val qty = $(_.qty)
          }

          Widget.qty.remove(0)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("containsElement only works on a set field, not a list") {
        // Lists.scala documents this claim in a comment; Catalog.names (already-compiled,
        // top-level) is the same List[String] field referenced there.
        typeCheck("""
          import examples.highlevelapi.Lists._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Catalog.names.containsElement("x")
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      // The remaining six all reuse Scalars.Widget.id (already-compiled, top-level, a plain
      // String field) as the "wrong shape" target, with an argument of the same String type
      // so only the Allows check fails — not an incidental argument-type mismatch.
      test("increment only works on a numeric field") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.id.increment("x")
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("decrement only works on a numeric field") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.id.decrement("x")
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("add only works on a numeric field") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.id.add("x")
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("addSet only works on a set field") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.id.addSet("x")
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("deleteFromSet only works on a set field") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.id.deleteFromSet("x")
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("appendList only works on a list field") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.id.appendList(Seq("x"))
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test("prependList only works on a list field") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.id.prependList(Seq("x"))
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("allows"))))
      },
      test(".contains only works on a String field") {
        typeCheck("""
          import zio.blocks.schema.{ CompanionOptics, Schema }
          import zio.dynamodb.blocks.ddbexpr.dsl._

          final case class Widget(id: String, qty: Int)
          object Widget extends CompanionOptics[Widget] {
            implicit val schema: Schema[Widget] = Schema.derived
            val qty = $(_.qty)
          }

          Widget.qty.contains("1")
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("member"))))
      },
      test("between/in/inSet don't work on a plain AnyVal value class") {
        // Sku/Widget come from WrappedScalars.scala (already-compiled, top-level) rather than
        // being defined fresh in this snippet: a case class extending AnyVal can't be a local
        // class under Scala 2, and typeCheck compiles its snippet in a local scope.
        //
        // The two Scala versions reject this for different reasons: Scala 3 resolves the
        // `between` extension method and then rejects it via the Allows shape check; Scala 2
        // never resolves the extension method at all ("value between is not a member of
        // Lens[...]"). Either way it fails to compile.
        typeCheck("""
          examples.highlevelapi.WrappedScalars.Widget.sku.between(
            examples.highlevelapi.WrappedScalars.Sku("a"),
            examples.highlevelapi.WrappedScalars.Sku("z")
          )
        """).map { result =>
          val messageIndicatesRejection = result.swap.exists { msg =>
            val lower = msg.toLowerCase
            lower.contains("allows") || lower.contains("not a member")
          }
          assertTrue(result.isLeft, messageIndicatesRejection)
        }
      }
    ),
    suite("PrimaryKey vs Extended")(
      // get/update/deleteFrom address a single item, so they require a DdbKeyExpr.PrimaryKey
      // (partition-only, or Composite — partition + sort equality). A sort-key range
      // (Extended) doesn't identify one item, and query is the only operation that accepts
      // it. Event/events come from KeyConditions.scala (already-compiled, top-level).
      //
      // The Extended key expression is bound to its own `val` before being passed in, rather
      // than written inline — inline, get's expected parameter type (PrimaryKey[Event]) leaks
      // into overload resolution of the `&&` call that builds it, so the reported error is
      // about the wrong `&&` overload instead of the PrimaryKey/Extended mismatch this test
      // means to check.
      test("get rejects an Extended (sort-key range) key") {
        typeCheck("""
          import examples.highlevelapi.KeyConditions._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val keyExpr = Event.streamId.partitionKey === "s1" && Event.seq.sortKey > 1L
          get(events)(keyExpr)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("primarykey"))))
      },
      test("update rejects an Extended (sort-key range) key") {
        typeCheck("""
          import examples.highlevelapi.KeyConditions._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val keyExpr = Event.streamId.partitionKey === "s1" && Event.seq.sortKey > 1L
          update(events)(keyExpr)(Event.payload.set("x"))
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("primarykey"))))
      },
      test("deleteFrom rejects an Extended (sort-key range) key") {
        typeCheck("""
          import examples.highlevelapi.KeyConditions._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val keyExpr = Event.streamId.partitionKey === "s1" && Event.seq.sortKey > 1L
          deleteFrom(events)(keyExpr)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("primarykey"))))
      },
      test("but get accepts a Composite (partition + sort equality) key") {
        typeCheck("""
          import examples.highlevelapi.KeyConditions._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          get(events)(Event.streamId.partitionKey === "s1" && Event.seq.sortKey === 1L)
        """).map(result => assertTrue(result.isRight))
      }
    ),
    suite("where vs filter")(
      // `.where`/`.filter` are both generic methods on the core DynamoDBQuery itself, gated
      // by the CanWhere/CanFilter compile-time proofs (not by which HL builder method exists)
      // — CanWhere only holds when Out matches a put/update/delete result shape, CanFilter
      // only when Out is a Page (scan/query). Calling the wrong one on a builder still
      // compiles as far as method lookup goes (the builder converts to a DynamoDBQuery
      // implicitly), so it's the proof, not a missing method, that rejects it.
      //
      // The condition is built with the type-unsafe, low-level `$` API (`ConditionExpression`/
      // `FilterExpression` are the same type — see core/package.scala) rather than a
      // Schema-derived `Widget.qty > 0` (a `SchemaExpr`, the wrong argument type entirely).
      // It's explicitly typed as `ConditionExpression[Widget]`: `$` has no `Schema` to bind
      // to, so left to infer on its own it produces `ConditionExpression[Any]` — and since
      // that type parameter is contravariant, an `Any`-typed condition is usable as a
      // `FilterExpression[B]` for *any* `B`, which would let the compiler pick `B = Out` and
      // satisfy CanFilter/CanWhere via their trivial reflexive case. That's not a gap in the
      // proof, just the expected result of not giving it a real model type to check —
      // pinning the type here is what makes the intended per-model check apply at all.
      test(".filter is rejected on a put") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.ProjectionExpression
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val ce: zio.dynamodb.ConditionExpression[Widget] = ProjectionExpression.$("qty") > 0
          put(widgets, Widget("w-1", 1, 1.0, active = true, zio.blocks.chunk.Chunk[Byte](1))).filter(ce)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("filter"))))
      },
      test(".filter is rejected on an update") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.ProjectionExpression
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val ce: zio.dynamodb.ConditionExpression[Widget] = ProjectionExpression.$("qty") > 0
          update(widgets)(Widget.id.partitionKey === "w-1")(Widget.qty.set(1)).filter(ce)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("filter"))))
      },
      test(".where is rejected on a scan") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.ProjectionExpression
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val ce: zio.dynamodb.ConditionExpression[Widget] = ProjectionExpression.$("qty") > 0
          scan(widgets, limit = 20).where(ce)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("put, update"))))
      },
      test(".where is rejected on a query") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.ProjectionExpression
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val ce: zio.dynamodb.ConditionExpression[Widget] = ProjectionExpression.$("qty") > 0
          query(widgets, limit = 20).whereKey(Widget.id.partitionKey === "w-1").where(ce)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("put, update"))))
      },
      test("but .where compiles fine on a put") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.ProjectionExpression
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val ce: zio.dynamodb.ConditionExpression[Widget] = ProjectionExpression.$("qty") > 0
          put(widgets, Widget("w-1", 1, 1.0, active = true, zio.blocks.chunk.Chunk[Byte](1))).where(ce)
        """).map(result => assertTrue(result.isRight))
      },
      // `get`, unlike put/update/deleteFrom/scan/query, has no builder at all — it returns
      // the bare core DynamoDBQuery directly. So `.filter` on a `get` result resolves to the
      // raw core method, which only accepts a real ConditionExpression, not a DdbExpr — a
      // plain argument-type mismatch, not CanFilter.
      test("get rejects a DdbExpr passed to .filter (get has no builder to convert it)") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          get(widgets)(Widget.id.partitionKey === "w-1").filter(sBetween && nLte)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("conditionexpression"))))
      },
      // Even with a real ConditionExpression, .filter on a get is still rejected — this time
      // genuinely via CanFilter, since get's Out (Either[ItemError, Widget]) isn't Page-shaped.
      // Filtering a single-item lookup has no meaning either way.
      test("get rejects .filter even with a real ConditionExpression (Out isn't Page-shaped)") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.ProjectionExpression
          import zio.dynamodb.blocks.ddbexpr.dsl._

          val ce: zio.dynamodb.ConditionExpression[Widget] = ProjectionExpression.$("qty") > 0
          get(widgets)(Widget.id.partitionKey === "w-1").filter(ce)
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("filter"))))
      }
    ),
    suite("value must match the optic's own type")(
      // No DDB-specific mechanism here — plain Scala overload resolution / type-checking on
      // ===/set/addSet's own type parameter, same as calling any other generic method with
      // the wrong argument type. Included for completeness: it's the most basic layer this
      // catalog covers, underneath Allows/CanWhere-CanFilter/PrimaryKey-Extended.
      test("=== rejects a value of the wrong type") {
        typeCheck("""
          import examples.highlevelapi.Scalars._

          Widget.id === 42
        """).map(result => assertTrue(result.isLeft))
      },
      test(".set rejects a value of the wrong type") {
        typeCheck("""
          import examples.highlevelapi.Scalars._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Widget.qty.set("wrong")
        """).map(result => assertTrue(result.isLeft))
      },
      test("addSet rejects a set of the wrong element type") {
        typeCheck("""
          import examples.highlevelapi.Sets._
          import zio.dynamodb.blocks.ddbexpr.dsl._

          Inventory.tags.addSet(Set(1, 2, 3))
        """).map(result => assertTrue(result.isLeft, result.swap.exists(_.toLowerCase.contains("string"))))
      },
      test("but === with a value of the right type compiles fine") {
        typeCheck("""
          import examples.highlevelapi.Scalars._

          Widget.id === "w-1"
        """).map(result => assertTrue(result.isRight))
      }
    )
  )
}
