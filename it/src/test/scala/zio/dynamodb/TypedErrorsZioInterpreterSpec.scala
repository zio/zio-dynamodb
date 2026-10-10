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
import software.amazon.awssdk.services.dynamodb.model.{ ConditionalCheckFailedException, ResourceNotFoundException }
import zio._
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion._
import zio.test._

object TypedErrorsZioInterpreterSpec extends DynamoDBLocalSpec {

  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  private def conditionalPut(table: String) =
    DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 99)).where($("score") === 42)

  private val existingItem: Option[Item] = Some(Item("id" -> "a", "score" -> 0))

  private def conditionFailedWithScore(score: Int): Assertion[Any] =
    isSubtype[DynamoFailure.ConditionFailed](
      hasField[DynamoFailure.ConditionFailed, Option[Item]](
        "item",
        _.item,
        isSome(equalTo(Item("id" -> "a", "score" -> score)))
      )
    )

  private val optionCTests =
    suite("option C: conditionFailedAsLeft")(
      test("plain ZIO interpreter: the Left is a value, typed Task[Either[ConditionFailed, _]]") {
        withSingleIdKeyTable { (table, plain) =>
          implicit val i: Interpreter[Task]                                            = plain
          val put: Task[Either[DynamoDBError.ItemError.ConditionFailed, Option[Item]]] =
            conditionalPut(table)
              .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
              .conditionFailedAsLeft
              .execute
          for {
            _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 0)))
            result <- put
          } yield assert(result)(isLeft(hasField("item", _.item, equalTo(existingItem))))
        }
      },
      test("CE interpreter: the Left is a value, typed IO[Either[ConditionFailed, _]]") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val ce: CEInterpreter[cats.effect.IO] =
            CEInterpreter.fromAsyncClient[cats.effect.IO](env.asyncClient)
          withSingleIdKeyTable { (table, plain) =>
            val put: cats.effect.IO[Either[DynamoDBError.ItemError.ConditionFailed, Option[Item]]] =
              conditionalPut(table)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
                .conditionFailedAsLeft
                .execute
            for {
              _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 0)))
              result <- ZIO.fromFuture(_ => put.unsafeToFuture()(cats.effect.unsafe.implicits.global))
            } yield assert(result)(isLeft(hasField("item", _.item, equalTo(existingItem))))
          }
        }
      },
      test("typed ZIO interpreter: the Left is flattened into the channel, typed IO[DynamoFailure, _]") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val i: TypedErrorsZioInterpreter = ZioInterpreter.fromAsyncClientWithTypedErrors(env.asyncClient)
          withSingleIdKeyTable { (table, plain) =>
            val put: IO[DynamoFailure, Option[Item]] =
              conditionalPut(table)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
                .conditionFailedAsLeft
                .execute
            for {
              _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 0)))
              result <- put.either
            } yield assert(result)(isLeft(conditionFailedWithScore(0)))
          }
        }
      },
      test("typed ZIO interpreter: success flattens to the plain value") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val i: TypedErrorsZioInterpreter = ZioInterpreter.fromAsyncClientWithTypedErrors(env.asyncClient)
          withSingleIdKeyTable { (table, plain) =>
            for {
              _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 42)))
              result <- conditionalPut(table).conditionFailedAsLeft.execute
            } yield assertTrue(result.isEmpty)
          }
        }
      },
      test("typed ZIO interpreter: a .map composed on the Either still sees a real Either") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val i: TypedErrorsZioInterpreter = ZioInterpreter.fromAsyncClientWithTypedErrors(env.asyncClient)
          withSingleIdKeyTable { (table, plain) =>
            val put: IO[DynamoFailure, String] =
              conditionalPut(table)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
                .conditionFailedAsLeft
                .map {
                  case Left(cf) => Left(cf)
                  case Right(_) => Right("written")
                }
                .execute
            for {
              _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 0)))
              result <- put.either
            } yield assert(result)(isLeft(conditionFailedWithScore(0)))
          }
        }
      }
    )

  def spec =
    suite("TypedErrorsZioInterpreter")(
      optionCTests,
      test("success returns the value; .execute is typed IO[DynamoFailure, _]") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val i: TypedErrorsZioInterpreter = ZioInterpreter.fromAsyncClientWithTypedErrors(env.asyncClient)
          withSingleIdKeyTable { (table, plain) =>
            val get: IO[DynamoFailure, Option[Item]] = DynamoDBQuery.getItem(table, PrimaryKey("id" -> "a")).execute
            for {
              _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 1)))
              result <- get
            } yield assertTrue(result.contains(Item("id" -> "a", "score" -> 1)))
          }
        }
      },
      test("failed condition with AllOld → ConditionFailed carrying the item") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val i: TypedErrorsZioInterpreter = ZioInterpreter.fromAsyncClientWithTypedErrors(env.asyncClient)
          withSingleIdKeyTable { (table, plain) =>
            for {
              _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 0)))
              result <- conditionalPut(table)
                          .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
                          .execute
                          .either
            } yield assert(result)(isLeft(conditionFailedWithScore(0)))
          }
        }
      },
      test("cancelled transaction → Library(TransactionCancelled), translated through the classifying bridge") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val i: TypedErrorsZioInterpreter = ZioInterpreter.fromAsyncClientWithTypedErrors(env.asyncClient)
          withSingleIdKeyTable { (table, plain) =>
            for {
              _      <- plain.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 0)))
              result <- DynamoDBQuery
                          .transactWriteItems(
                            DynamoDBQuery.putItem(table, Item("id" -> "a", "score" -> 99)).where($("score") === 42)
                          )
                          .execute
                          .either
            } yield assert(result)(
              isLeft(
                isSubtype[DynamoFailure.Library](
                  hasField("error", _.error, isSubtype[DynamoDBError.TransactionError.TransactionCancelled](anything))
                )
              )
            )
          }
        }
      },
      test("missing table → Rejected(ResourceNotFoundException)") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val i: TypedErrorsZioInterpreter = ZioInterpreter.fromAsyncClientWithTypedErrors(env.asyncClient)
          for {
            result <- DynamoDBQuery.getItem("no-such-table", PrimaryKey("id" -> "a")).execute.either
          } yield assert(result)(
            isLeft(
              isSubtype[DynamoFailure.Rejected](
                hasField("error", _.error, isSubtype[ResourceNotFoundException](anything))
              )
            )
          )
        }
      }
    ).provideSomeLayer[DynamoDbAsyncClient](envLayer) @@ TestAspect.sequential
}
