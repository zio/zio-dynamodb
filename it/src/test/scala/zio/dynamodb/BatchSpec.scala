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
import zio.test._
import zio.test.Assertion.{ anything, isSubtype }

import java.util.UUID

object BatchSpec extends DynamoDBLocalSpec {

  private def withTable(
    interp: AwsInterpreter[Task]
  )(
    f: (String, AwsInterpreter[Task]) => ZIO[Any, Throwable, TestResult]
  ): Task[TestResult] =
    ZIO.scoped {
      for {
        tableName <- ZIO.succeed(s"test-bu-${UUID.randomUUID()}")
        _         <- interp.run(singleIdKeyTable(tableName))
        _         <- ZIO.addFinalizer(interp.run(DynamoDBQuery.deleteTable(tableName)).orDie)
        result    <- f(tableName, interp)
      } yield result
    }

  def spec = suite("Batch IT")(
    suite("runWriteItem")(
      test("returns Complete when all items processed") {
        for {
          client <- ZIO.service[DynamoDbAsyncClient]
          interp = ZioInterpreter.fromAsyncClient(client)
          result <- withTable(interp) { (table, interp) =>
                      val items = List(
                        Item("id" -> "a", "v" -> 1),
                        Item("id" -> "b", "v" -> 2),
                        Item("id" -> "c", "v" -> 3)
                      )
                      Batch
                        .runWriteItem(interp)(
                          DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i))
                        )
                        .map(r => assert(r)(isSubtype[Batch.WriteResult.Complete](anything)))
                    }
        } yield result
      },

      test("written items are retrievable individually") {
        for {
          client <- ZIO.service[DynamoDbAsyncClient]
          interp = ZioInterpreter.fromAsyncClient(client)
          result <- withTable(interp) { (table, interp) =>
                      val items = List(Item("id" -> "x", "v" -> 10), Item("id" -> "y", "v" -> 20))
                      for {
                        _  <- Batch.runWriteItem(interp)(
                                DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i))
                              )
                        rx <- interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "x")))
                        ry <- interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "y")))
                      } yield assertTrue(
                        rx.contains(Item("id" -> "x", "v" -> 10)) &&
                          ry.contains(Item("id" -> "y", "v" -> 20))
                      )
                    }
        } yield result
      },

      test("batch delete removes all targeted items") {
        for {
          client <- ZIO.service[DynamoDbAsyncClient]
          interp = ZioInterpreter.fromAsyncClient(client)
          result <- withTable(interp) { (table, interp) =>
                      val items = List(Item("id" -> "d1"), Item("id" -> "d2"))
                      val keys  = List(PrimaryKey("id" -> "d1"), PrimaryKey("id" -> "d2"))
                      for {
                        _   <- Batch.runWriteItem(interp)(
                                 DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i))
                               )
                        _   <- Batch.runWriteItem(interp)(
                                 DynamoDBQuery.batchWriteItem(keys)(k => DynamoDBQuery.deleteItem(table, k))
                               )
                        rd1 <- interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "d1")))
                        rd2 <- interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "d2")))
                      } yield assertTrue(rd1.isEmpty && rd2.isEmpty)
                    }
        } yield result
      }
    ),

    suite("runGetItem")(
      test("returns Complete when all items exist") {
        for {
          client <- ZIO.service[DynamoDbAsyncClient]
          interp = ZioInterpreter.fromAsyncClient(client)
          result <- withTable(interp) { (table, interp) =>
                      val ids   = List("p", "q", "r")
                      val items = ids.map(id => Item("id" -> id))
                      for {
                        _      <- Batch.runWriteItem(interp)(
                                    DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i))
                                  )
                        result <-
                          Batch.runGetItem(interp)(
                            DynamoDBQuery.batchGetItem(ids)(id => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> id)))
                          )
                      } yield assert(result)(isSubtype[Batch.GetResult.Complete](anything))
                    }
        } yield result
      },

      test("all requested items are present in the response") {
        for {
          client <- ZIO.service[DynamoDbAsyncClient]
          interp = ZioInterpreter.fromAsyncClient(client)
          result <- withTable(interp) { (table, interp) =>
                      val ids   = List("m", "n")
                      val items = ids.map(id => Item("id" -> id, "score" -> 42))
                      for {
                        _      <- Batch.runWriteItem(interp)(
                                    DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i))
                                  )
                        result <-
                          Batch.runGetItem(interp)(
                            DynamoDBQuery.batchGetItem(ids)(id => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> id)))
                          )
                        found = result match {
                                  case Batch.GetResult.Complete(r) => r.responses.getOrElse(table, Set.empty)
                                  case _                           => Set.empty
                                }
                      } yield assertTrue(found.size == 2)
                    }
        } yield result
      },

      test("absent keys return Complete with empty responses") {
        for {
          client <- ZIO.service[DynamoDbAsyncClient]
          interp = ZioInterpreter.fromAsyncClient(client)
          result <- withTable(interp) { (table, interp) =>
                      val ids = List("ghost-1", "ghost-2")
                      Batch
                        .runGetItem(interp)(
                          DynamoDBQuery.batchGetItem(ids)(id => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> id)))
                        )
                        .map(r => assert(r)(isSubtype[Batch.GetResult.Complete](anything)))
                    }
        } yield result
      }
    )
  )
}
