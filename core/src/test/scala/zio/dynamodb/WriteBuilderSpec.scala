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

  private val check: ConditionCheckBuilder[Any] = DynamoDBQuery.conditionCheck("t", PrimaryKey("id" -> "a"))(condition)

  private def itemOnFailureIs(rv: ReturnValuesOnConditionCheckFailure): Assertion[DynamoDBQuery[_, _]] =
    isSubtype[DynamoDBQuery.ConditionCheck](
      hasField[DynamoDBQuery.ConditionCheck, Option[ReturnValuesOnConditionCheckFailure]](
        "returnValuesOnConditionCheckFailure",
        _.returnValuesOnConditionCheckFailure,
        isSome(equalTo(rv))
      )
    )

  // The write node under any Map / builder wrapping.
  @scala.annotation.tailrec
  private def leaf(q: DynamoDBQuery[_, _]): DynamoDBQuery[_, _] = q match {
    case DynamoDBQuery.Map(inner, _) => leaf(inner)
    case w: WriteNode[_, _]          => leaf(w.write)
    case other                       => other
  }

  private val leafHasItemOnFailure: Assertion[DynamoDBQuery[_, _]] =
    hasField[DynamoDBQuery[_, _], DynamoDBQuery[_, _]](
      "leaf",
      leaf,
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
      val returning: ItemWriteBuilder[Any, Option[Item], WriteOp.Put] = put.returning(ReturnValues.AllOld)
      val item                                                        = Some(Item("id" -> "a"))
      assert(returning.write)(
        isSubtype[PutItem](hasField[PutItem, ReturnValues]("returnValues", _.returnValues, equalTo(allOld)))
      ) &&
      assertTrue(returning.finish(item) == item)
    },
    test("the general modifiers keep the builder, in any order with .where / .returning") {
      val b = put
        .capacity(total)
        .returning(ReturnValues.AllOld)
        .metrics(size)
        .where(condition)
        .withRetryPolicy(RetryPolicy.NoRetry)
      assert(b.write)(
        isSubtype[PutItem](
          hasField[PutItem, ReturnConsumedCapacity]("capacity", _.capacity, equalTo(total)) &&
            hasField[PutItem, ReturnItemCollectionMetrics]("itemMetrics", _.itemMetrics, equalTo(size)) &&
            hasField[PutItem, ReturnValues]("returnValues", _.returnValues, equalTo(allOld)) &&
            hasField[PutItem, Option[RetryPolicy]]("retryPolicy", _.retryPolicy, isSome(anything))
        )
      )
    },
    suite("ConditionCheckBuilder")(
      test("returns the existing item on failure by default") {
        assert(check.check)(itemOnFailureIs(ReturnValuesOnConditionCheckFailure.AllOld))
      },
      test(".withoutItemOnConditionFailure turns it off") {
        assert(check.withoutItemOnConditionFailure.check)(itemOnFailureIs(ReturnValuesOnConditionCheckFailure.None))
      },
      test("general modifiers keep the builder type but fail, since a condition check has none of them") {
        val afterModifiers: ConditionCheckBuilder[Any] =
          check.capacity(total).metrics(size).withRetryPolicy(RetryPolicy.NoRetry).withoutItemOnConditionFailure
        assert(afterModifiers.check)(
          isSubtype[DynamoDBQuery.Fail](
            hasField(
              "error",
              _.error(),
              isSubtype[DynamoDBError.QueryBuilderError.UnsupportedModifier](
                hasField("message", _.message, containsString("'.capacity'"))
              )
            )
          )
        )
      },
      test("runs inside a transaction") {
        val tx = DynamoDBQuery.transactWriteItems(check.withoutItemOnConditionFailure, put)
        assertTrue(DummyIOInterpreter.run(tx).unsafeRun() == (()))
      }
    ),
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
    suite(".where on a composed write keeps the item-on-failure default")(
      test("after .map") {
        assert(put.map(identity).where(condition))(leafHasItemOnFailure)
      },
      test("after .toQuery") {
        assert(put.toQuery.where(condition))(leafHasItemOnFailure)
      },
      test("on both sides of a zipPar") {
        val pair = (put zipPar DynamoDBQuery.putItem("t", Item("id" -> "b"))).where(condition)
        assert(pair)(
          isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]]("left", _.left, leafHasItemOnFailure) &&
              hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]]("right", _.right, leafHasItemOnFailure)
          )
        )
      }
    ),
    test("toQuery maps the raw result through finish") {
      assertTrue(DummyIOInterpreter.run(put.toQuery).unsafeRun() == (()))
    }
  )
}
