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

import zio.dynamodb.DynamoDBQuery.PutItem
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion._
import zio.test._

object WriteBuilderSpec extends ZIOSpecDefault {

  private val put: ItemWriteBuilder[Any, Unit, WriteOp.Put] = DynamoDBQuery.putItem("t", Item("id" -> "a"))

  private val condition: ConditionExpression[Any] = $("v") === 0
  private val total: ReturnConsumedCapacity       = ReturnConsumedCapacity.Total
  private val size: ReturnItemCollectionMetrics   = ReturnItemCollectionMetrics.Size
  private val allOld: ReturnValues                = ReturnValues.AllOld

  def spec = suite("WriteBuilder")(
    test(".where sets the condition and returns the existing item on failure by default") {
      assert(put.where(condition).write)(
        isSubtype[PutItem](
          hasField[PutItem, Option[ConditionExpression[_]]](
            "conditionExpression",
            _.conditionExpression,
            isSome(anything)
          ) &&
            hasField[PutItem, Option[ReturnValuesOnConditionCheckFailure]](
              "returnValuesOnConditionCheckFailure",
              _.returnValuesOnConditionCheckFailure,
              isSome(equalTo(ReturnValuesOnConditionCheckFailure.AllOld))
            )
        )
      )
    },
    test(".withoutItemOnConditionFailure turns the default off") {
      assert(put.where(condition).withoutItemOnConditionFailure.write)(
        isSubtype[PutItem](
          hasField[PutItem, Option[ReturnValuesOnConditionCheckFailure]](
            "returnValuesOnConditionCheckFailure",
            _.returnValuesOnConditionCheckFailure,
            isSome(equalTo(ReturnValuesOnConditionCheckFailure.None))
          )
        )
      )
    },
    test(".returning sets ReturnValues and makes the returned item the result") {
      val returning: ItemWriteBuilder[Any, Option[Item], WriteOp.Put] = put.returning(allOld)
      val item                                                        = Some(Item("id" -> "a"))
      assert(returning.write)(
        isSubtype[PutItem](hasField[PutItem, ReturnValues]("returnValues", _.returnValues, equalTo(allOld)))
      ) &&
      assertTrue(returning.finish(item) == item)
    },
    test("the general modifiers keep the builder, in any order with .where / .returning") {
      val b = put.capacity(total).returning(allOld).metrics(size).where(condition).withRetryPolicy(RetryPolicy.NoRetry)
      assert(b.write)(
        isSubtype[PutItem](
          hasField[PutItem, ReturnConsumedCapacity]("capacity", _.capacity, equalTo(total)) &&
            hasField[PutItem, ReturnItemCollectionMetrics]("itemMetrics", _.itemMetrics, equalTo(size)) &&
            hasField[PutItem, ReturnValues]("returnValues", _.returnValues, equalTo(allOld)) &&
            hasField[PutItem, Option[RetryPolicy]]("retryPolicy", _.retryPolicy, isSome(anything))
        )
      )
    },
    test("conditionCheck returns the existing item on failure by default; withoutItemOnConditionFailure turns it off") {
      val check = DynamoDBQuery.conditionCheck("t", PrimaryKey("id" -> "a"))(condition)
      assertTrue(
        check.returnValuesOnConditionCheckFailure.contains(ReturnValuesOnConditionCheckFailure.AllOld),
        check.withoutItemOnConditionFailure.returnValuesOnConditionCheckFailure
          .contains(ReturnValuesOnConditionCheckFailure.None)
      )
    },
    test("a builder is itself the query - no wrapper is allocated") {
      assertTrue(put.toQuery eq put)
    },
    test("general modifiers reach builders nested in zipPar, and the pair still runs") {
      val pair: DynamoDBQuery[Any, Unit]                            =
        (put zipPar DynamoDBQuery.deleteItem("t", PrimaryKey("id" -> "b")))
          .capacity(total)
          .withRetryPolicy(RetryPolicy.NoRetry)
      def node(q: DynamoDBQuery[_, _]): Option[DynamoDBQuery[_, _]] = q match {
        case w: WriteNode[_, _] => Some(w.write)
        case _                  => None
      }
      val (left, right)                                             = pair match {
        case DynamoDBQuery.ZipPar(l, r, _) => (node(l), node(r))
        case _                             => (None, None)
      }
      assert(left)(
        isSome(
          isSubtype[PutItem](
            hasField[PutItem, ReturnConsumedCapacity]("capacity", _.capacity, equalTo(total)) &&
              hasField[PutItem, Option[RetryPolicy]]("retryPolicy", _.retryPolicy, isSome(anything))
          )
        )
      ) &&
      assert(right)(
        isSome(
          isSubtype[DynamoDBQuery.DeleteItem](
            hasField[DynamoDBQuery.DeleteItem, ReturnConsumedCapacity]("capacity", _.capacity, equalTo(total))
          )
        )
      ) &&
      assertTrue(DummyIOInterpreter.run(pair).unsafeRun() == (()))
    },
    test("toQuery maps the raw result through finish") {
      assertTrue(DummyIOInterpreter.run(put.toQuery).unsafeRun() == (()))
    }
  )
}
