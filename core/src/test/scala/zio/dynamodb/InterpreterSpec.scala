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

import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.SetOperand.ValueOperand
import zio.test._
import zio.test.Assertion.{ anything, containsString, hasField, isNone, isSome, isSubtype, isTrue }

object InterpreterSpec extends ZIOSpecDefault {

  // Unwrap DummyIO synchronously — readable shorthand for tests
  // TODO: run real ZIO and CE interpreters against this same suite of laws
  private def eval[A](q: DynamoDBQuery[_, A]): A =
    DummyIOInterpreter.run(q).unsafeRun()

  def spec = suite("DummyIOInterpreter")(
    suite("Succeed law — run(succeed(a)) returns a")(
      test("int value") {
        assertTrue(eval(DynamoDBQuery(42)) == 42)
      },
      test("string value") {
        assertTrue(eval(DynamoDBQuery("hello")) == "hello")
      }
    ),

    suite("Map laws (functor)")(
      test("identity — map(identity) is a no-op") {
        val q = DynamoDBQuery(42)
        assertTrue(eval(q.map(identity)) == eval(q))
      },
      test("composition — map(f).map(g) equals map(f andThen g)") {
        val q                = DynamoDBQuery(42)
        val f: Int => Int    = _ + 1
        val g: Int => String = _.toString
        assertTrue(eval(q.map(f).map(g)) == eval(q.map(f andThen g)))
      }
    ),

    suite("ZipPar laws")(
      test("produces both results as a tuple") {
        val (l, r) = eval(DynamoDBQuery(1) zipPar DynamoDBQuery("a"))
        assertTrue(l == 1 && r == "a")
      },
      test("zipParLeft discards right result") {
        assertTrue(eval(DynamoDBQuery(1).zipParLeft(DynamoDBQuery(2))) == 1)
      },
      test("zipParRight discards left result") {
        assertTrue(eval(DynamoDBQuery(1).zipParRight(DynamoDBQuery(2))) == 2)
      },
      test("zipPar then map equals zipParWith") {
        val q1 = DynamoDBQuery(3)
        val q2 = DynamoDBQuery(4)
        assertTrue(
          eval((q1 zipPar q2).map { case (a, b) => a + b }) ==
            eval(q1.zipParWith(q2)(_ + _))
        )
      },
      test("independently zipped GetItem queries each return None") {
        val q1       = DynamoDBQuery.GetItem("t1", PrimaryKey("id" -> "1"))
        val q2       = DynamoDBQuery.GetItem("t2", PrimaryKey("id" -> "2"))
        val (r1, r2) = eval(q1 zipPar q2)
        assertTrue(r1.isEmpty && r2.isEmpty)
      }
    ),

    suite("Fail law")(
      test("propagates DynamoDBError directly as the thrown exception") {
        val error   = ItemError.ValueNotFound("not found")
        val attempt = scala.util.Try(eval(DynamoDBQuery.Fail(() => error)))
        assert(attempt.failed.get)(isSubtype[DynamoDBError](anything))
      }
    ),

    suite("Absolve law")(
      test("Right unwraps the value") {
        val q = DynamoDBQuery(Right(42): Either[ItemError, Int])
        assertTrue(eval(DynamoDBQuery.Absolve(q)) == 42)
      },
      test("Left propagates DynamoDBError directly as the thrown exception") {
        val error   = ItemError.ValueNotFound("missing")
        val q       = DynamoDBQuery(Left(error): Either[ItemError, Int])
        val attempt = scala.util.Try(eval(DynamoDBQuery.Absolve(q)))
        assert(attempt.failed.get)(isSubtype[DynamoDBError](anything))
      }
    ),

    suite("DummyIO table management stubs")(
      test("CreateTable returns Unit") {
        val attrs = NonEmptySet(AttributeDefinition.attrDefnString("id"))
        eval(DynamoDBQuery.createTable("t", KeySchema("id"), attrs, BillingMode.PayPerRequest))
        assertTrue(true)
      },
      test("DeleteTable returns Unit") {
        eval(DynamoDBQuery.deleteTable("t"))
        assertTrue(true)
      },
      test("DescribeTable returns a stub response") {
        val resp = eval(DynamoDBQuery.describeTable("t"))
        assertTrue(resp.tableStatus == DynamoDBQuery.TableStatus.Active)
      }
    ),

    suite("DummyIO stub responses")(
      test("GetItem always returns None") {
        val q = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "1"))
        assertTrue(eval(q).isEmpty)
      },
      test("PutItem always returns None") {
        val q = DynamoDBQuery.putItem("t", Item("k" -> "v"))
        assertTrue(eval(q).isEmpty)
      },
      test("UpdateItem always returns None") {
        val key = PrimaryKey("id" -> "1")
        val q   = DynamoDBQuery.updateItem("t", key)(ProjectionExpression.$("name").set("alice"))
        assertTrue(eval(q).isEmpty)
      },
      test("DeleteItem always returns None") {
        val q = DynamoDBQuery.deleteItem("t", PrimaryKey("id" -> "1"))
        assertTrue(eval(q).isEmpty)
      },
      test("Query returns empty page") {
        val page = eval(DynamoDBQuery.Query("t", limit = 10))
        assertTrue(page.items.isEmpty && page.lastEvaluatedKey.isEmpty)
      },
      test("Scan returns empty page") {
        val page = eval(DynamoDBQuery.scan("t", limit = 10))
        assertTrue(page.items.isEmpty && page.lastEvaluatedKey.isEmpty)
      },
      test("BatchGetItem returns a Complete result with an empty response") {
        val q = DynamoDBQuery.BatchGetItem()
        val r = eval(q)
        assert(r)(
          isSubtype[Batch.GetResult.Complete](
            hasField("responses empty", _.response.responses.iterator.isEmpty, isTrue)
          )
        )
      },
      test("BatchWriteItem returns a Complete result with an empty response") {
        val q = DynamoDBQuery.BatchWriteItem()
        val r = eval(q)
        assert(r)(
          isSubtype[Batch.WriteResult.Complete](hasField("unprocessedItems", _.response.unprocessedItems, isNone))
        )
      }
    ),

    suite("DynamoDBQuery constructors")(
      test("succeed constructs a Succeed query") {
        assertTrue(eval(DynamoDBQuery.succeed(99)) == 99)
      },
      test("fail constructs a Fail query that throws a DynamoDBError on run") {
        val error   = DynamoDBError.ItemError.ValueNotFound("oops")
        val q       = DynamoDBQuery.fail(error)
        val attempt = scala.util.Try(eval(q))
        assert(attempt.failed.get)(isSubtype[DynamoDBError](anything))
      },
      test("apply constructs a Succeed query") {
        assertTrue(eval(DynamoDBQuery(42)) == 42)
      },
      test("getItem with projection list") {
        val q = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "1"), ProjectionExpression.$("name"))
        assertTrue(eval(q).isEmpty)
      },
      test("deleteItem with default params returns None") {
        val q = DynamoDBQuery.deleteItem("t", PrimaryKey("id" -> "1"))
        assertTrue(eval(q).isEmpty)
      }
    ),

    // Regression coverage for a transact-write validation gap: a standalone `updateItem`
    // rejects an UpdateExpression.Action.Failure via validateAction before running, but
    // runAny's TransactWriteItems case only checked condition-expression failures — an
    // UpdateItem's own action failure reached toAwsTransactWriteItem unchecked.
    // Action.collectActions silently drops a Failure node when rendering a composed action
    // (see its own comment: "guarded by validateAction in the interpreter"), so a mix of one
    // valid and one failing `.set` would render — and submit to AWS — only the valid half,
    // with no error and no trace of the dropped one. Fixed by extending the transact-write
    // failure check to also run UpdateExpression.collectFailures on every UpdateItem's action.
    suite("TransactWriteItems — UpdateItem action-failure validation")(
      test("a bare Action.Failure in a transactional UpdateItem fails before running, not silently") {
        val badUpdate = DynamoDBQuery.UpdateItem(
          tableName = "orders",
          key = PrimaryKey("id" -> "o1"),
          updateExpression = UpdateExpression(Action.Failure("only String keys are supported in DDB"))
        )
        val tx        = DynamoDBQuery.transactWriteItems(
          badUpdate,
          DynamoDBQuery.putItem("other", Item("id" -> "o2"))
        )
        val thrown    = scala.util.Try(eval(tx)).failed.toOption
        assert(thrown)(
          isSome(
            isSubtype[DynamoDBError.ItemError.DecodingError](
              hasField(
                "message",
                (_: DynamoDBError.ItemError.DecodingError).message,
                containsString("only String keys are supported in DDB")
              )
            )
          )
        )
      },
      test("a composed action (valid SET + Failure) fails as a whole, instead of rendering only the valid half") {
        val goodPlusBad = Action.SetAction($("status"), ValueOperand(AttributeValue.String("shipped"))) +
          Action.Failure("found map key '1' — only String keys are supported in DDB")
        val badUpdate   = DynamoDBQuery.UpdateItem(
          tableName = "orders",
          key = PrimaryKey("id" -> "o1"),
          updateExpression = UpdateExpression(goodPlusBad)
        )
        val thrown      = scala.util.Try(eval(DynamoDBQuery.transactWriteItems(badUpdate))).failed.toOption
        assert(thrown)(
          isSome(
            isSubtype[DynamoDBError.ItemError.DecodingError](
              hasField(
                "message",
                (_: DynamoDBError.ItemError.DecodingError).message,
                containsString("only String keys are supported in DDB")
              )
            )
          )
        )
      },
      test("an HL-shaped Map(UpdateItem-with-Failure, decoder) is unwrapped and still caught") {
        // Mirrors what DdbExprApi.update actually produces at the ADT level — a Map node
        // wrapping the real UpdateItem, not the bare UpdateItem a Low-Level caller would build.
        val badUpdate = DynamoDBQuery.UpdateItem(
          tableName = "orders",
          key = PrimaryKey("id" -> "o1"),
          updateExpression = UpdateExpression(Action.Failure("bad optic path"))
        )
        val hlShaped  = DynamoDBQuery.Map[Option[Item], Option[Item]](badUpdate, identity)
        val thrown    = scala.util.Try(eval(DynamoDBQuery.transactWriteItems(hlShaped))).failed.toOption
        assert(thrown)(isSome(isSubtype[DynamoDBError.ItemError.DecodingError](anything)))
      },
      test("a transaction with only valid actions is unaffected by the added check") {
        val goodUpdate = DynamoDBQuery.UpdateItem(
          tableName = "orders",
          key = PrimaryKey("id" -> "o1"),
          updateExpression =
            UpdateExpression(Action.SetAction($("status"), ValueOperand(AttributeValue.String("shipped"))))
        )
        assertTrue(eval(DynamoDBQuery.transactWriteItems(goodUpdate)) == (()))
      }
    )
  )
}
