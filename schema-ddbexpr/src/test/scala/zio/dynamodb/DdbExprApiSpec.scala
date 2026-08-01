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
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbExprApi, DdbKeyExpr }
import zio.dynamodb.blocks.ddbexpr.DdbExprApi._
// derivedCodec from DdbKeyExpr._; bring DdbExpr extension methods separately
// to avoid the two-derivedCodec ambiguity with DdbExpr.derivedCodec.
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.dynamodb.blocks.ddbexpr.DdbExpr.{ DdbExprBoolSyntax, OpticDdbExprOps, OpticStringDdbExprOps }
import zio.test._
import zio.test.Assertion._

object DdbExprApiSpec extends ZIOSpecDefault {

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

  // ── Helpers ──────────────────────────────────────────────────────────────────

  // Executes a DynamoDBQuery synchronously through DummyIOInterpreter.
  // runGetItem always returns None — exercises the not-found path in get.
  private def run[A](q: DynamoDBQuery[_, A]): A = DummyIOInterpreter.run(q).unsafeRun()

  // ── Spec ─────────────────────────────────────────────────────────────────────

  def spec = suite("DdbExprApi")(
    suite("item encoding round-trip")(
      test("codec decodes a well-formed item") {
        val codec = implicitly[zio.dynamodb.blocks.schema.DynamoDBCodec[Task]]
        val task  = Task("t1", 42, Priority.High)
        val av    = AttributeValue.Map(
          Map(
            AttributeValue.String("id")       -> AttributeValue.String("t1"),
            AttributeValue.String("score")    -> AttributeValue.Number(BigDecimal(42)),
            AttributeValue.String("priority") -> AttributeValue.String("High")
          )
        )
        assertTrue(codec.decoder(av) == Right(task))
      },
      test("sealed-trait with all-no-field cases encodes as AttributeValue.String") {
        val codec = implicitly[zio.dynamodb.blocks.schema.DynamoDBCodec[Task]]
        val task  = Task("t1", 42, Priority.High)
        val av    = codec.encoder(task)
        av match {
          case AttributeValue.Map(m) =>
            assertTrue(m.get(AttributeValue.String("priority")).contains(AttributeValue.String("High")))
          case other                 =>
            assertNever(s"expected Map, got $other")
        }
      }
    ),
    suite("put — encode-failure path")(
      test("put on a non-record schema (encodes to a non-Map AttributeValue) fails, not silently drops the write") {
        implicit val intSchema: Schema[Int] = Schema.int
        val q                               = DdbExprApi.put[Int]("tasks", 5)
        assert(q)(isSubtype[DynamoDBQuery.Fail](anything))
      }
    ),
    suite("get — via DummyIOInterpreter")(
      test("returns ValueNotFound when item is absent") {
        val q      = DdbExprApi.get[Task]("tasks")(Task.id.partitionKey === "t1")
        val result = run(q)
        assert(result)(isLeft(isSubtype[ItemError.ValueNotFound](anything)))
      },
      test("ValueNotFound message contains the primary key value") {
        val q      = DdbExprApi.get[Task]("tasks")(Task.id.partitionKey === "alice")
        val result = run(q)
        assert(result)(isLeft(hasField("message", (_: ItemError).message, containsString("alice"))))
      }
      // Range expressions (sk > / between / beginsWith) on get are now a compile-time error:
      // DdbExprApi.get takes DdbKeyExpr.PrimaryKey, which Extended does not satisfy.
    ),
    suite("implicit conversions")(
      test("DdbKeyExpr converts to PartitionKeyEquals (not Failure)") {
        val kce = ddbKeyExprToKeyConditionExpr[Task](Task.id.partitionKey === "alice")
        assert(kce)(isSubtype[KeyConditionExpr.PartitionKeyEquals[Task]](anything))
      },
      test("DdbKeyExpr composite converts to CompositePrimaryKeyExpr") {
        val kce = ddbKeyExprToKeyConditionExpr[Task](Task.id.partitionKey === "alice" && Task.score.sortKey === 42)
        assert(kce)(isSubtype[KeyConditionExpr.CompositePrimaryKeyExpr[Task]](anything))
      },
      test("DdbExpr converts to non-Failure ConditionExpression") {
        val ce = ddbExprToConditionExpression[Task](Task.id === "alice")
        assert(ce)(not(isSubtype[ConditionExpression.Failure[Task]](anything)))
      }
    ),
    suite("query / scan chaining")(
      test("query builds successfully with .whereKey and .filter (===)") {
        val q = DdbExprApi
          .query[Task]("tasks", 20)
          .whereKey(Task.id.partitionKey === "alice" && Task.score.sortKey > 10)
          .filter(Task.priority === Priority.High)
        assertTrue(q != null)
      },
      test("scan builds successfully with .filter via SchemaExpr") {
        val q = DdbExprApi
          .scan[Task]("tasks", 20)
          .filter(Task.score > 0)
        assertTrue(q != null)
      }
    ),
    suite(".where — condition expression on write operations")(
      suite("put.where")(
        test("put.where with DdbExpr (===) runs without error") {
          val q = DdbExprApi
            .put("tasks", Task("t1", 42, Priority.High))
            .where(Task.priority === Priority.High)
          assertTrue(run(q).isEmpty)
        },
        test("put.where with SchemaExpr (Builtin path) runs without error") {
          val q = DdbExprApi
            .put("tasks", Task("t1", 42, Priority.High))
            .where(Task.score > 0)
          assertTrue(run(q).isEmpty)
        },
        test("put.where with combined && expression runs without error") {
          val q = DdbExprApi
            .put("tasks", Task("t1", 42, Priority.High))
            .where((Task.priority === Priority.High) && (Task.score > 0))
          assertTrue(run(q).isEmpty)
        },
        test("put.where with attributeExists runs without error") {
          val q = DdbExprApi
            .put("tasks", Task("t1", 42, Priority.High))
            .where(Task.id.attributeExists)
          assertTrue(run(q).isEmpty)
        },
        test("put.where with Failure CE raises DecodingError on run") {
          val q      = DdbExprApi
            .put("tasks", Task("t1", 42, Priority.High))
            .where(ConditionExpression.Failure[Task]("version check failed"))
          val thrown = scala.util.Try(run(q)).failed.toOption
          assertTrue(thrown.exists(_.isInstanceOf[DynamoDBError.ItemError.DecodingError]))
        },
        test("put.where Failure message is preserved in thrown error") {
          val q      = DdbExprApi
            .put("tasks", Task("t1", 42, Priority.High))
            .where(ConditionExpression.Failure[Task]("specific message"))
          val thrown = scala.util.Try(run(q)).failed.toOption
          assertTrue(thrown.exists(_.getMessage.contains("specific message")))
        },
        test("multiple Failure nodes accumulate via collectFailures") {
          val ce   = ConditionExpression.Failure[Task]("err1") && ConditionExpression.Failure[Task]("err2")
          val msgs = ConditionExpression.collectFailures(ce)
          assertTrue(msgs == List("err1", "err2"))
        }
      ),
      suite("deleteFrom.where")(
        test("deleteFrom.where with DdbExpr (===) runs without error") {
          val q = DdbExprApi
            .deleteFrom[Task]("tasks")(Task.id.partitionKey === "t1")
            .where(Task.priority === Priority.High)
          assertTrue(run(q).isEmpty)
        },
        test("deleteFrom.where with SchemaExpr runs without error") {
          val q = DdbExprApi
            .deleteFrom[Task]("tasks")(Task.id.partitionKey === "t1")
            .where(Task.score > 0)
          assertTrue(run(q).isEmpty)
        },
        test("deleteFrom.where Failure CE raises DecodingError on run") {
          val q      = DdbExprApi
            .deleteFrom[Task]("tasks")(Task.id.partitionKey === "t1")
            .where(ConditionExpression.Failure[Task]("precondition failed"))
          val thrown = scala.util.Try(run(q)).failed.toOption
          assertTrue(thrown.exists(_.isInstanceOf[DynamoDBError.ItemError.DecodingError]))
        }
      )
    )
  )

  private def assertNever(msg: String) = assertTrue(msg == "impossible")
}
