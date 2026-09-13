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
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.SetOperand.ValueOperand
import zio.test._
import zio.test.Assertion.{ anything, containsString, hasField, isSome, isSubtype }

/**
 * Regression coverage for a transact-write validation gap: a standalone `updateItem` rejects
 * an `UpdateExpression.Action.Failure` via `validateAction` before running, but `runAny`'s
 * `TransactWriteItems` case only checked condition-expression failures — an `UpdateItem`'s own
 * action failure reached `toAwsTransactWriteItem` unchecked. `Action.collectActions` silently
 * drops a `Failure` node when rendering a composed action (see its own comment: "guarded by
 * validateAction in the interpreter"), so a mix of one valid and one failing `.set` would
 * render — and submit to AWS — only the valid half, with no error and no trace of the dropped
 * one. Fixed by extending the transact-write failure check to also run
 * `UpdateExpression.collectFailures` on every `UpdateItem`'s action.
 */
object TransactWriteItemsValidationSpec extends ZIOSpecDefault {

  private def run[A](q: DynamoDBQuery[_, A]): A = DummyIOInterpreter.run(q).unsafeRun()

  private def failedDecodingError[A](q: DynamoDBQuery[_, A]) = scala.util.Try(run(q)).failed.toOption

  def spec = suite("TransactWriteItems — UpdateItem action-failure validation")(
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
      val thrown    = failedDecodingError(tx)
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
      val thrown      = failedDecodingError(DynamoDBQuery.transactWriteItems(badUpdate))
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
      val thrown    = failedDecodingError(DynamoDBQuery.transactWriteItems(hlShaped))
      assert(thrown)(isSome(isSubtype[DynamoDBError.ItemError.DecodingError](anything)))
    },
    test("a transaction with only valid actions is unaffected by the added check") {
      val goodUpdate = DynamoDBQuery.UpdateItem(
        tableName = "orders",
        key = PrimaryKey("id" -> "o1"),
        updateExpression =
          UpdateExpression(Action.SetAction($("status"), ValueOperand(AttributeValue.String("shipped"))))
      )
      assertTrue(run(DynamoDBQuery.transactWriteItems(goodUpdate)) == (()))
    }
  )
}
