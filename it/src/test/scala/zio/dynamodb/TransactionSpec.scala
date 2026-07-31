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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.dynamodb.DynamoDBError.TransactionError
import zio.dynamodb.ProjectionExpression.$
import zio.test._
import zio.test.Assertion.{ anything, equalTo, hasField, isLeft, isNone, isSome, isSubtype }

object TransactionSpec extends DynamoDBLocalSpec {

  private val zioEnvLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  // ---------------------------------------------------------------------------
  // transactGetItems happy paths
  // ---------------------------------------------------------------------------

  private val transactGetTests: Spec[DynamoDBEnv, Throwable] =
    suite("transactGetItems")(
      test("reads two existing items — results match what was put, in order") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "v" -> 1)))
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "b", "v" -> 2)))
            results <- interpreter.run(
                         DynamoDBQuery.transactGetItems(
                           DynamoDBQuery.GetItem(table, PrimaryKey("id" -> "a")),
                           DynamoDBQuery.GetItem(table, PrimaryKey("id" -> "b"))
                         )
                       )
          } yield assertTrue(
            results(0).contains(Item("id" -> "a", "v" -> 1)) &&
              results(1).contains(Item("id" -> "b", "v" -> 2))
          )
        }
      },
      test("item not found → None at that position; existing item → Some with correct content") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "present", "v" -> 7)))
            results <- interpreter.run(
                         DynamoDBQuery.transactGetItems(
                           DynamoDBQuery.GetItem(table, PrimaryKey("id" -> "present")),
                           DynamoDBQuery.GetItem(table, PrimaryKey("id" -> "absent"))
                         )
                       )
          } yield assertTrue(
            results(0).contains(Item("id" -> "present", "v" -> 7)) &&
              results(1).isEmpty
          )
        }
      }
    )

  // ---------------------------------------------------------------------------
  // transactWriteItems happy paths
  // ---------------------------------------------------------------------------

  private val transactWriteTests: Spec[DynamoDBEnv, Throwable] =
    suite("transactWriteItems")(
      test("puts two items atomically — both readable with correct content after") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _  <- interpreter.run(
                    DynamoDBQuery.transactWriteItems(
                      DynamoDBQuery.putItem(table, Item("id" -> "tx-a", "v" -> 1)),
                      DynamoDBQuery.putItem(table, Item("id" -> "tx-b", "v" -> 2))
                    )
                  )
            ra <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "tx-a")))
            rb <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "tx-b")))
          } yield assertTrue(
            ra.contains(Item("id" -> "tx-a", "v" -> 1)) &&
              rb.contains(Item("id" -> "tx-b", "v" -> 2))
          )
        }
      },
      test("update + delete atomically — updated item exists, deleted item gone") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "keep", "v" -> 0)))
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "drop")))
            _       <- interpreter.run(
                         DynamoDBQuery.transactWriteItems(
                           DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "keep"))($("v").set(42)),
                           DynamoDBQuery.deleteItem(table, PrimaryKey("id" -> "drop"))
                         )
                       )
            kept    <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "keep")))
            dropped <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "drop")))
          } yield assertTrue(
            kept.contains(Item("id" -> "keep", "v" -> 42)) &&
              dropped.isEmpty
          )
        }
      },
      test("condition check passes — write proceeds") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _ <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "guarded", "status" -> "open")))
            _ <- interpreter.run(
                   DynamoDBQuery.transactWriteItems(
                     DynamoDBQuery.conditionCheck(table, PrimaryKey("id" -> "guarded"))($("status") === "open"),
                     DynamoDBQuery.putItem(table, Item("id" -> "new-item"))
                   )
                 )
            r <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "new-item")))
          } yield assertTrue(r.contains(Item("id" -> "new-item")))
        }
      },
      test("idempotency: same clientRequestToken produces same result on replay") {
        withSingleIdKeyTable { (table, interpreter) =>
          val token = java.util.UUID.randomUUID().toString
          val tx    = DynamoDBQuery
            .transactWriteItems(
              DynamoDBQuery.putItem(table, Item("id" -> "idem", "v" -> 1))
            )
            .withClientRequestToken(token)
          for {
            _ <- interpreter.run(tx)
            _ <- interpreter.run(tx) // replay with same token
            r <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "idem")))
          } yield assertTrue(r.contains(Item("id" -> "idem", "v" -> 1)))
        }
      }
    )

  // ---------------------------------------------------------------------------
  // transactWriteItems error paths
  // ---------------------------------------------------------------------------

  private val transactWriteErrorTests: Spec[DynamoDBEnv, Throwable] =
    suite("transactWriteItems error paths")(
      test("failed condition check → error message contains TransactionCancelled and ConditionalCheckFailed") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "locked", "status" -> "closed")))
            result <- interpreter
                        .run(
                          DynamoDBQuery.transactWriteItems(
                            // condition requires status=open but it's closed → will fail
                            DynamoDBQuery.conditionCheck(table, PrimaryKey("id" -> "locked"))($("status") === "open"),
                            DynamoDBQuery.putItem(table, Item("id" -> "should-not-write"))
                          )
                        )
                        .either
          } yield assert(result)(
            // DynamoDB returns one reason per action: conditionCheck failed, putItem was not attempted ("None")
            isLeft(
              isSubtype[TransactionError.TransactionCancelled](
                hasField(
                  "reasons.length",
                  (tc: TransactionError.TransactionCancelled) => tc.reasons.length,
                  equalTo(2)
                ) &&
                  hasField(
                    "reasons(0).code",
                    (tc: TransactionError.TransactionCancelled) => tc.reasons(0).code,
                    equalTo("ConditionalCheckFailed")
                  ) &&
                  hasField(
                    "reasons(0).item",
                    (tc: TransactionError.TransactionCancelled) => tc.reasons(0).item,
                    isNone
                  ) &&
                  hasField(
                    "reasons(1).code",
                    (tc: TransactionError.TransactionCancelled) => tc.reasons(1).code,
                    equalTo("None")
                  )
              )
            )
          )
        }
      },
      test("failed condition with AllOld — cancellation reason message reflects attempted write was rejected") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "item", "v" -> 0)))
            result <- interpreter
                        .run(
                          DynamoDBQuery.transactWriteItems(
                            DynamoDBQuery
                              .updateItem(table, PrimaryKey("id" -> "item"))($("v").set(1))
                              .where($("v") === 999) // impossible condition
                              .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
                          )
                        )
                        .either
          } yield assert(result)(
            isLeft(
              isSubtype[TransactionError.TransactionCancelled](
                hasField(
                  "reasons.length",
                  (tc: TransactionError.TransactionCancelled) => tc.reasons.length,
                  equalTo(1)
                ) &&
                  hasField(
                    "reasons(0).code",
                    (tc: TransactionError.TransactionCancelled) => tc.reasons(0).code,
                    equalTo("ConditionalCheckFailed")
                  ) &&
                  hasField(
                    "reasons(0).item",
                    (tc: TransactionError.TransactionCancelled) => tc.reasons(0).item,
                    isSome(anything)
                  )
              )
            )
          )
        }
      }
    )

  def spec = suite("TransactionSpec")(
    suite("ZIO interpreter")(transactGetTests, transactWriteTests, transactWriteErrorTests)
      .provideSome[DynamoDbAsyncClient](zioEnvLayer)
  )
}
