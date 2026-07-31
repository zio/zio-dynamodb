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

import cats.effect.unsafe.implicits.{ global => ceRuntime }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.blocks.chunk.Chunk
import zio.dynamodb.ProjectionExpression.$
import zio.test._
import zio.test.TestAspect

object DynamoDBLowLevelApiSpec extends DynamoDBLocalSpec {

  // Bridges a CE interpreter into Task so the same test suite can run it.
  private def ceBridge(ceInterp: CEInterpreter): Interpreter[Task] =
    new Interpreter[Task] {
      def run[A](q: DynamoDBQuery[_, A]): Task[A] =
        ZIO.fromFuture(_ => ceInterp.run(q).unsafeToFuture()(ceRuntime))
    }

  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private val zioEnvLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  private val ceEnvLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(
      ZIO.serviceWith[DynamoDbAsyncClient](client =>
        DynamoDBEnv(client, ceBridge(CEInterpreter.fromAsyncClient(client)))
      )
    )

  private def futureBridge(interp: FutureInterpreter): Interpreter[Task] =
    new Interpreter[Task] {
      def run[A](q: DynamoDBQuery[_, A]): Task[A] =
        ZIO.fromFuture(_ => interp.run(q)).mapError {
          case e: java.util.concurrent.CompletionException if e.getCause != null => e.getCause
          case e                                                                 => e
        }
    }

  private val futureEnvLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(
      ZIO.serviceWith[DynamoDbAsyncClient](client =>
        DynamoDBEnv(client, futureBridge(FutureInterpreter.fromAsyncClient(client)))
      )
    )

  // ---------------------------------------------------------------------------
  // Single partition key tests
  // ---------------------------------------------------------------------------

  private val singleKeyTests: Spec[DynamoDBEnv, Throwable] =
    suite("single partition key")(
      test("getItem returns None for a missing key") {
        withSingleIdKeyTable { (table, interpreter) =>
          interpreter
            .run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "missing")))
            .map(result => assertTrue(result.isEmpty))
        }
      },
      test("putItem then getItem roundtrip") {
        withSingleIdKeyTable { (table, interpreter) =>
          val item = Item("id" -> "alice", "score" -> 42)
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, item))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice")))
          } yield assertTrue(result.contains(Item("id" -> "alice", "score" -> 42)))
        }
      },
      test("putItem overwrites an existing item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "v" -> "first")))
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "v" -> "second")))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bob")))
          } yield assertTrue(result.contains(Item("id" -> "bob", "v" -> "second")))
        }
      },
      test("deleteItem removes an item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "charlie")))
            _      <- interpreter.run(DynamoDBQuery.deleteItem(table, PrimaryKey("id" -> "charlie")))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "charlie")))
          } yield assertTrue(result.isEmpty)
        }
      },
      test("scanSome returns all inserted items") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "a")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "b")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c")))
            page <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10))
          } yield assertTrue(page.items.length == 3)
        }
      },
      test("createTable/describeTable/deleteTable lifecycle") {
        for {
          env  <- ZIO.service[DynamoDBEnv]
          tableName = s"lifecycle-${java.util.UUID.randomUUID()}"
          attrs = NonEmptySet(AttributeDefinition.attrDefnString("id"))
          _    <-
            env.interpreter.run(DynamoDBQuery.createTable(tableName, KeySchema("id"), attrs, BillingMode.PayPerRequest))
          desc <- env.interpreter.run(DynamoDBQuery.describeTable(tableName))
          _    <- env.interpreter.run(DynamoDBQuery.deleteTable(tableName))
        } yield assertTrue(desc.tableStatus == DynamoDBQuery.TableStatus.Active)
      },
      test("zipped gets execute independently") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "x")))
            q = DynamoDBQuery.getItem(table, PrimaryKey("id" -> "x")) zipPar
                  DynamoDBQuery.getItem(table, PrimaryKey("id" -> "y"))
            pair <- interpreter.run(q)
            (rx, ry) = pair
          } yield assertTrue(rx.isDefined && ry.isEmpty)
        }
      },
      test("zipPar getItem results are returned in a tuple with correct values") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "score" -> 10)))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "score" -> 20)))
            q = DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice")) zipPar
                  DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bob"))
            pair <- interpreter.run(q)
            (ra, rb) = pair
          } yield assertTrue(
            ra.flatMap(_.getOption[Int]("score")).contains(10) &&
              rb.flatMap(_.getOption[Int]("score")).contains(20)
          )
        }
      },
      test("zipPar three queries returns a 3-tuple with correct values") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c1", "v" -> 1)))
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c2", "v" -> 2)))
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c3", "v" -> 3)))
            q = DynamoDBQuery.getItem(table, PrimaryKey("id" -> "c1")) zipPar
                  DynamoDBQuery.getItem(table, PrimaryKey("id" -> "c2")) zipPar
                  DynamoDBQuery.getItem(table, PrimaryKey("id" -> "c3"))
            triple <- interpreter.run(q)
            (r1, r2, r3) = triple
          } yield assertTrue(
            r1.flatMap(_.getOption[Int]("v")).contains(1) &&
              r2.flatMap(_.getOption[Int]("v")).contains(2) &&
              r3.flatMap(_.getOption[Int]("v")).contains(3)
          )
        }
      },
      test("getItem with projection returns only requested attributes") {
        withSingleIdKeyTable { (table, interpreter) =>
          val item = Item("id" -> "proj-alice", "score" -> 42, "extra" -> "excluded")
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, item))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "proj-alice"), $("score")))
          } yield assertTrue(
            result.exists(_.map.contains("score")) &&
              result.map(_.map.contains("extra")).exists(!_)
          )
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Composite partition+sort key tests (id + year)
  // ---------------------------------------------------------------------------

  private val compositeKeyTests: Spec[DynamoDBEnv, Throwable] =
    suite("composite partition+sort key")(
      test("getItem returns None for a missing composite key") {
        withIdAndYearKeyTable { (table, interpreter) =>
          interpreter
            .run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "missing", "year" -> "2024")))
            .map(result => assertTrue(result.isEmpty))
        }
      },
      test("putItem then getItem roundtrip with composite key") {
        withIdAndYearKeyTable { (table, interpreter) =>
          val item = Item("id" -> "alice", "year" -> "2024", "score" -> 42)
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, item))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice", "year" -> "2024")))
          } yield assertTrue(result.contains(Item("id" -> "alice", "year" -> "2024", "score" -> 42)))
        }
      },
      test("same partition key with different sort keys produces distinct items") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023", "score" -> 10)))
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2024", "score" -> 20)))
            r2023 <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice", "year" -> "2023")))
            r2024 <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice", "year" -> "2024")))
          } yield assertTrue(
            r2023.contains(Item("id" -> "alice", "year" -> "2023", "score" -> 10)) &&
              r2024.contains(Item("id" -> "alice", "year" -> "2024", "score" -> 20))
          )
        }
      },
      test("putItem overwrites item with same composite key") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2024", "v" -> "first")))
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2024", "v" -> "second")))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bob", "year" -> "2024")))
          } yield assertTrue(result.contains(Item("id" -> "bob", "year" -> "2024", "v" -> "second")))
        }
      },
      test("deleteItem removes only the targeted sort key, leaving sibling intact") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "charlie", "year" -> "2023")))
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "charlie", "year" -> "2024")))
            _       <- interpreter.run(DynamoDBQuery.deleteItem(table, PrimaryKey("id" -> "charlie", "year" -> "2023")))
            deleted <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "charlie", "year" -> "2023")))
            intact  <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "charlie", "year" -> "2024")))
          } yield assertTrue(deleted.isEmpty && intact.isDefined)
        }
      },
      test("scanSome returns all items across different partition and sort keys") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "year" -> "2023")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "b", "year" -> "2022")))
            page <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10))
          } yield assertTrue(page.items.length == 3)
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Batch write tests
  // ---------------------------------------------------------------------------

  private val batchingTests: Spec[DynamoDBEnv, Throwable] =
    suite("batchWriteItem")(
      test("batch put — all items readable after write") {
        withSingleIdKeyTable { (table, interpreter) =>
          val items = List(
            Item("id" -> "a", "v" -> 1),
            Item("id" -> "b", "v" -> 2),
            Item("id" -> "c", "v" -> 3)
          )
          for {
            _  <- interpreter.run(DynamoDBQuery.batchWriteItem(items)(item => DynamoDBQuery.putItem(table, item)))
            ra <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "a")))
            rb <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "b")))
            rc <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "c")))
          } yield assertTrue(
            ra.contains(Item("id" -> "a", "v" -> 1)) &&
              rb.contains(Item("id" -> "b", "v" -> 2)) &&
              rc.contains(Item("id" -> "c", "v" -> 3))
          )
        }
      },
      test("batch delete — items absent after sequential put then delete batch") {
        withSingleIdKeyTable { (table, interpreter) =>
          val items = List(Item("id" -> "x"), Item("id" -> "y"))
          val keys  = List(PrimaryKey("id" -> "x"), PrimaryKey("id" -> "y"))
          for {
            _  <- interpreter.run(DynamoDBQuery.batchWriteItem(items)(item => DynamoDBQuery.putItem(table, item)))
            _  <- interpreter.run(DynamoDBQuery.batchWriteItem(keys)(key => DynamoDBQuery.deleteItem(table, key)))
            rx <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "x")))
            ry <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "y")))
          } yield assertTrue(rx.isEmpty && ry.isEmpty)
        }
      },
      test("mixed puts and deletes in one batch") {
        withSingleIdKeyTable { (table, interpreter) =>
          val writes: List[Either[Item, PrimaryKey]] = List(
            Left(Item("id" -> "keep-me", "v" -> 42)),
            Right(PrimaryKey("id" -> "del-me"))
          )
          for {
            _       <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "del-me")))
            _       <- interpreter.run(DynamoDBQuery.batchWriteItem(writes) {
                         case Left(item) => DynamoDBQuery.putItem(table, item)
                         case Right(key) => DynamoDBQuery.deleteItem(table, key)
                       })
            kept    <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "keep-me")))
            deleted <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "del-me")))
          } yield assertTrue(kept.contains(Item("id" -> "keep-me", "v" -> 42)) && deleted.isEmpty)
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Batch get tests
  // ---------------------------------------------------------------------------

  private val batchGetItemTests: Spec[DynamoDBEnv, Throwable] =
    suite("batchGetItem")(
      test("batch get returns all items that were put") {
        withSingleIdKeyTable { (table, interpreter) =>
          val items = List(
            Item("id" -> "a", "v" -> 1),
            Item("id" -> "b", "v" -> 2),
            Item("id" -> "c", "v" -> 3)
          )
          val batch =
            DynamoDBQuery.batchGetItem(List("a", "b", "c"))(id => DynamoDBQuery.getItem(table, PrimaryKey("id" -> id)))
          for {
            _        <- interpreter.run(DynamoDBQuery.batchWriteItem(items)(item => DynamoDBQuery.putItem(table, item)))
            response <- interpreter.run(batch)
            results = batch.toGetItemResponses(response)
          } yield assertTrue(
            results.length == 3 &&
              results.forall(_.isDefined)
          )
        }
      },
      test("batch get returns None for keys that do not exist") {
        withSingleIdKeyTable { (table, interpreter) =>
          val batch = DynamoDBQuery.batchGetItem(List("missing-1", "missing-2"))(id =>
            DynamoDBQuery.getItem(table, PrimaryKey("id" -> id))
          )
          for {
            response <- interpreter.run(batch)
            results = batch.toGetItemResponses(response)
          } yield assertTrue(results.length == 2 && results.forall(_.isEmpty))
        }
      },
      test("batch get mixed — present key resolves to Some, absent key resolves to None") {
        withSingleIdKeyTable { (table, interpreter) =>
          val batch = DynamoDBQuery.batchGetItem(List("present", "absent"))(id =>
            DynamoDBQuery.getItem(table, PrimaryKey("id" -> id))
          )
          for {
            _        <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "present", "v" -> 42)))
            response <- interpreter.run(batch)
            results = batch.toGetItemResponses(response)
          } yield assertTrue(
            results.length == 2 &&
              results(0).isDefined &&
              results(1).isEmpty
          )
        }
      }
    )

  // ---------------------------------------------------------------------------
  // UpdateItem tests
  // ---------------------------------------------------------------------------

  private val updateItemTests: Spec[DynamoDBEnv, Throwable] =
    suite("updateItem")(
      test("set overwrites an existing attribute") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "score" -> 0)))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "alice")) {
                          $("score").set(42)
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice")))
          } yield assertTrue(result.contains(Item("id" -> "alice", "score" -> 42)))
        }
      },
      test("set adds a new attribute to an existing item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob")))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "bob")) {
                          $("score").set(99)
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bob")))
          } yield assertTrue(result.contains(Item("id" -> "bob", "score" -> 99)))
        }
      },
      test("update with condition succeeds when condition matches") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "charlie", "score" -> 0)))
            _      <- interpreter.run(
                        DynamoDBQuery
                          .updateItem(table, PrimaryKey("id" -> "charlie")) {
                            $("score").set(42)
                          }
                          .where($("score") === 0)
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "charlie")))
          } yield assertTrue(result.contains(Item("id" -> "charlie", "score" -> 42)))
        }
      },
      test("update with condition fails when condition does not match — item unchanged") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "dave", "score" -> 0)))
            result <- interpreter
                        .run(
                          DynamoDBQuery
                            .updateItem(table, PrimaryKey("id" -> "dave")) {
                              $("score").set(99)
                            }
                            .where($("score") === 100)
                        )
                        .either
            item   <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "dave")))
          } yield assertTrue(result.isLeft && item.contains(Item("id" -> "dave", "score" -> 0)))
        }
      },
      test("remove deletes an attribute from an item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "eve", "score" -> 42, "tmp" -> "x")))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "eve")) {
                          UpdateExpression.Action.RemoveAction($("tmp"))
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "eve")))
          } yield assertTrue(result.contains(Item("id" -> "eve", "score" -> 42)))
        }
      },
      test("zipPar runs two updates to different rows concurrently") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _  <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "par-a", "score" -> 0)))
            _  <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "par-b", "score" -> 0)))
            q = DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "par-a"))($("score").set(1)) zipPar
                  DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "par-b"))($("score").set(2))
            _  <- interpreter.run(q)
            ra <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "par-a")))
            rb <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "par-b")))
          } yield assertTrue(
            ra.contains(Item("id" -> "par-a", "score" -> 1)) &&
              rb.contains(Item("id" -> "par-b", "score" -> 2))
          )
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Condition expression (where) and filter expression (filter) tests
  // ---------------------------------------------------------------------------

  private val conditionAndFilterTests: Spec[DynamoDBEnv, Throwable] =
    suite("condition and filter expressions")(
      suite("where on putItem")(
        test("put succeeds when condition matches existing attribute value") {
          withSingleIdKeyTable { (table, interpreter) =>
            for {
              _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "score" -> 0)))
              _      <- interpreter.run(
                          DynamoDBQuery
                            .putItem(table, Item("id" -> "alice", "score" -> 1))
                            .where($("score") === 0)
                        )
              result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice")))
            } yield assertTrue(result.contains(Item("id" -> "alice", "score" -> 1)))
          }
        },
        test("put fails when condition does not match — item remains unchanged") {
          withSingleIdKeyTable { (table, interpreter) =>
            for {
              _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "score" -> 0)))
              result <- interpreter
                          .run(
                            DynamoDBQuery
                              .putItem(table, Item("id" -> "bob", "score" -> 99))
                              .where($("score") === 42)
                          )
                          .either
              item   <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bob")))
            } yield assertTrue(result.isLeft && item.contains(Item("id" -> "bob", "score" -> 0)))
          }
        }
      ),
      suite("where on deleteItem")(
        test("delete succeeds when condition matches") {
          withSingleIdKeyTable { (table, interpreter) =>
            for {
              _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "charlie", "score" -> 42)))
              _      <- interpreter.run(
                          DynamoDBQuery
                            .deleteItem(table, PrimaryKey("id" -> "charlie"))
                            .where($("score") === 42)
                        )
              result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "charlie")))
            } yield assertTrue(result.isEmpty)
          }
        },
        test("delete fails when condition does not match — item survives") {
          withSingleIdKeyTable { (table, interpreter) =>
            for {
              _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "dave", "score" -> 42)))
              fail <- interpreter
                        .run(
                          DynamoDBQuery
                            .deleteItem(table, PrimaryKey("id" -> "dave"))
                            .where($("score") === 0)
                        )
                        .either
              item <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "dave")))
            } yield assertTrue(fail.isLeft && item.isDefined)
          }
        }
      ),
      suite("filter on scanSome")(
        test("scan returns only items matching the filter") {
          withSingleIdKeyTable { (table, interpreter) =>
            for {
              _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "s1", "score" -> 1)))
              _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "s5", "score" -> 5)))
              _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "s10", "score" -> 10)))
              page <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10).filter($("score") > 5))
            } yield assertTrue(page.items.length == 1)
          }
        },
        test("scan with filter that matches nothing returns empty page") {
          withSingleIdKeyTable { (table, interpreter) =>
            for {
              _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "t1", "score" -> 1)))
              _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "t5", "score" -> 5)))
              page <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10).filter($("score") > 100))
            } yield assertTrue(page.items.isEmpty)
          }
        }
      )
    )

  // ---------------------------------------------------------------------------
  // querySome tests using whereKey and PE syntax
  // ---------------------------------------------------------------------------

  private val querySomeTests: Spec[DynamoDBEnv, Throwable] =
    suite("querySome with whereKey")(
      test("partition key only returns all items for that partition key") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2024")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2024")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice")
                    )
          } yield assertTrue(page.items.length == 3)
        }
      },
      test("composite equality returns exact item") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2024")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice" && $("year").sortKey === "2023")
                    )
          } yield assertTrue(
            page.items.length == 1 &&
              page.items.head == Item("id" -> "alice", "year" -> "2023")
          )
        }
      },
      test("sort key > returns items with sk greater than given value") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2021")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2024")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice" && $("year").sortKey > "2022")
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("sort key beginsWith returns only matching items") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022-Q1")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022-Q2")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023-Q1")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice" && $("year").sortKey.beginsWith("2022"))
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("querySome with whereKey and filter expression") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022", "score" -> 5)))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023", "score" -> 50)))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2024", "score" -> 100)))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice")
                        .filter($("score") > 10)
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("sortOrder(ascending = false) returns items in descending sort-key order") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2021")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice")
                        .sortOrder(ascending = false)
                    )
          } yield assertTrue(
            page.items.map(_.map.get("year")).toList ==
              List(
                Some(AttributeValue.String("2023")),
                Some(AttributeValue.String("2022")),
                Some(AttributeValue.String("2021"))
              )
          )
        }
      }
    )

  // ---------------------------------------------------------------------------
  // capacity() builder — smoke tests (verifies the full path doesn't break ops)
  // ---------------------------------------------------------------------------

  private val capacityTests: Spec[DynamoDBEnv, Throwable] =
    suite("capacity builder")(
      test("getItem with capacity(Total) still returns the correct item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "cap-a", "v" -> 1)))
            result <- interpreter.run(
                        DynamoDBQuery
                          .getItem(table, PrimaryKey("id" -> "cap-a"))
                          .capacity(ReturnConsumedCapacity.Total)
                      )
          } yield assertTrue(result.contains(Item("id" -> "cap-a", "v" -> 1)))
        }
      },
      test("putItem with capacity(Total) succeeds") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(
                        DynamoDBQuery
                          .putItem(table, Item("id" -> "cap-b"))
                          .capacity(ReturnConsumedCapacity.Total)
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "cap-b")))
          } yield assertTrue(result.isDefined)
        }
      },
      test("scanSome with capacity(Total) returns items") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "cap-s1")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "cap-s2")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .scanSome(table, limit = 10)
                        .capacity(ReturnConsumedCapacity.Total)
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("batchGetItem with capacity(Total) returns all items") {
        withSingleIdKeyTable { (table, interpreter) =>
          val items = List(Item("id" -> "cap-g1", "v" -> 1), Item("id" -> "cap-g2", "v" -> 2))
          val batch = DynamoDBQuery
            .batchGetItem(List("cap-g1", "cap-g2"))(id => DynamoDBQuery.getItem(table, PrimaryKey("id" -> id)))
            .capacity(ReturnConsumedCapacity.Total)
          for {
            _        <- interpreter.run(DynamoDBQuery.batchWriteItem(items)(i => DynamoDBQuery.putItem(table, i)))
            response <- interpreter.run(batch.asInstanceOf[DynamoDBQuery.BatchGetItem])
            results = batch.asInstanceOf[DynamoDBQuery.BatchGetItem].toGetItemResponses(response)
          } yield assertTrue(results.length == 2 && results.forall(_.isDefined))
        }
      }
    )

  private val consistencyTests: Spec[DynamoDBEnv, Throwable] =
    suite("consistency builder")(
      test("getItem with consistency(Strong) still returns the correct item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "con-a", "v" -> 1)))
            result <- interpreter.run(
                        DynamoDBQuery
                          .getItem(table, PrimaryKey("id" -> "con-a"))
                          .consistency(ConsistencyMode.Strong)
                      )
          } yield assertTrue(result.contains(Item("id" -> "con-a", "v" -> 1)))
        }
      },
      test("scanSome with consistency(Strong) returns items") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "con-s1")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .scanSome(table, limit = 10)
                        .consistency(ConsistencyMode.Strong)
                    )
          } yield assertTrue(page.items.nonEmpty)
        }
      }
    )

  private val returnsTests: Spec[DynamoDBEnv, Throwable] =
    suite("returns builder")(
      test("putItem with returns(AllOld) succeeds and returns old item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "ret-a", "v" -> 1)))
            result <- interpreter.run(
                        DynamoDBQuery
                          .putItem(table, Item("id" -> "ret-a", "v" -> 2))
                          .returns(ReturnValues.AllOld)
                      )
          } yield assertTrue(result.contains(Item("id" -> "ret-a", "v" -> 1)))
        }
      },
      test("deleteItem with returns(AllOld) returns the deleted item") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "ret-d", "v" -> 99)))
            result <- interpreter.run(
                        DynamoDBQuery
                          .deleteItem(table, PrimaryKey("id" -> "ret-d"))
                          .returns(ReturnValues.AllOld)
                      )
          } yield assertTrue(result.contains(Item("id" -> "ret-d", "v" -> 99)))
        }
      }
    )

  private val selectTests: Spec[DynamoDBEnv, Throwable] =
    suite("select and Page.count / Page.scannedCount")(
      test("Page.count equals items.size on a normal scan") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "a")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "b")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c")))
            page <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10))
          } yield assertTrue(page.count == page.items.size && page.count == 3)
        }
      },
      test("Page.scannedCount equals count when no filter is applied") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "x")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "y")))
            page <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10))
          } yield assertTrue(page.scannedCount == page.count)
        }
      },
      test("Page.scannedCount > count when a filter reduces the result set") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "s1", "v" -> 1)))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "s2", "v" -> 2)))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "s3", "v" -> 3)))
            page <- interpreter.run(
                      DynamoDBQuery.scanSome(table, limit = 10).filter($("v") > 1)
                    )
          } yield assertTrue(page.scannedCount == 3 && page.count == 2)
        }
      },
      test("selectCount: items is empty and count reflects matching items") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c1")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c2")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c3")))
            page <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10).selectCount)
          } yield assertTrue(page.items.isEmpty && page.count == 3)
        }
      },
      test("querySome selectCount: count reflects matching items for a partition key") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2021")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2021")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "alice")
                        .selectCount
                    )
          } yield assertTrue(page.items.isEmpty && page.count == 2)
        }
      }
    )

  private val gsiTests: Spec[DynamoDBEnv, Throwable] =
    suite("gsi builder")(
      test("querySome on a GSI returns only items matching the GSI partition key") {
        withGsiTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "category" -> "fruit")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "b", "category" -> "vegetable")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "c", "category" -> "fruit")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10, indexName = Some("category-index"))
                        .whereKey($("category").partitionKey === "fruit")
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("querySome on a GSI returns no items when partition key has no matches") {
        withGsiTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "a", "category" -> "fruit")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10, indexName = Some("category-index"))
                        .whereKey($("category").partitionKey === "mineral")
                    )
          } yield assertTrue(page.items.isEmpty)
        }
      }
    )

  private val lsiTests: Spec[DynamoDBEnv, Throwable] =
    suite("lsi builder")(
      test("querySome on an LSI returns items ordered by the LSI sort key") {
        withLsiTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2024", "score" -> "b")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023", "score" -> "a")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022", "score" -> "c")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10, indexName = Some("score-index"))
                        .whereKey($("id").partitionKey === "alice")
                    )
          } yield assertTrue(page.items.length == 3)
        }
      },
      test("querySome on an LSI with sort key condition returns matching items only") {
        withLsiTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2024", "score" -> "z")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2023", "score" -> "a")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10, indexName = Some("score-index"))
                        .whereKey($("id").partitionKey === "bob" && $("score").sortKey === "z")
                    )
          } yield assertTrue(page.items.length == 1)
        }
      }
    )

  private val startKeyTests: Spec[DynamoDBEnv, Throwable] =
    suite("startKey paging")(
      test("scanSome: second page starts after the last key of the first page") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _     <- interpreter.run(
                       DynamoDBQuery.batchWriteItem(List("sk-1", "sk-2", "sk-3"))(id =>
                         DynamoDBQuery.putItem(table, Item("id" -> id))
                       )
                     )
            page1 <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 1))
            page2 <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10).startKey(page1.lastEvaluatedKey))
          } yield assertTrue(
            page1.items.length == 1 &&
              page1.lastEvaluatedKey.isDefined &&
              page1.items.length + page2.items.length == 3
          )
        }
      },
      test("scanSome: lastEvaluatedKey is None on the final page") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "only")))
            page1 <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 1))
            page2 <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 10).startKey(page1.lastEvaluatedKey))
          } yield assertTrue(page2.items.isEmpty && page2.lastEvaluatedKey.isEmpty)
        }
      },
      test("querySome: second page starts after the last key of the first page") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2021")))
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2022")))
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "year" -> "2023")))
            page1 <- interpreter.run(
                       DynamoDBQuery
                         .querySome(table, limit = 1)
                         .whereKey($("id").partitionKey === "alice")
                     )
            page2 <- interpreter.run(
                       DynamoDBQuery
                         .querySome(table, limit = 10)
                         .whereKey($("id").partitionKey === "alice")
                         .startKey(page1.lastEvaluatedKey)
                     )
          } yield assertTrue(
            page1.items.length == 1 &&
              page1.lastEvaluatedKey.isDefined &&
              page1.items.length + page2.items.length == 3
          )
        }
      },
      test("querySome: pages are non-overlapping") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2021")))
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2022")))
            _     <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "year" -> "2023")))
            page1 <- interpreter.run(
                       DynamoDBQuery
                         .querySome(table, limit = 2)
                         .whereKey($("id").partitionKey === "bob")
                     )
            page2 <- interpreter.run(
                       DynamoDBQuery
                         .querySome(table, limit = 2)
                         .whereKey($("id").partitionKey === "bob")
                         .startKey(page1.lastEvaluatedKey)
                     )
            allItems = (page1.items ++ page2.items).map(_.getOption[String]("year"))
          } yield assertTrue(
            page1.items.length == 2 &&
              page2.items.length == 1 &&
              allItems.toSet.size == 3
          )
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Parallel scan segments
  // ---------------------------------------------------------------------------

  private val segmentTests: Spec[DynamoDBEnv, Throwable] =
    suite("parallel scan segments")(
      test("two segments together cover all items with no overlaps") {
        withSingleIdKeyTable { (table, interpreter) =>
          val ids = List("seg-1", "seg-2", "seg-3", "seg-4", "seg-5", "seg-6")
          for {
            _    <- interpreter.run(
                      DynamoDBQuery.batchWriteItem(ids)(id => DynamoDBQuery.putItem(table, Item("id" -> id)))
                    )
            seg0 <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 100).segment(0, 2))
            seg1 <- interpreter.run(DynamoDBQuery.scanSome(table, limit = 100).segment(1, 2))
            allIds = (seg0.items ++ seg1.items).flatMap(_.getOption[String]("id")).toSet
          } yield assertTrue(
            allIds == ids.toSet &&
              seg0.items.length + seg1.items.length == ids.length
          )
        }
      }
    )

  // ---------------------------------------------------------------------------
  // All DynamoDB attribute types
  // ---------------------------------------------------------------------------

  private val allAttributeTypesTests: Spec[DynamoDBEnv, Throwable] =
    suite("all attribute types")(
      test("scalar and collection types survive a put/get round-trip") {
        withSingleIdKeyTable { (table, interpreter) =>
          val item = Item(
            "id"     -> "types-all",
            "bin"    -> Chunk.fromArray("abc".getBytes),
            "bool"   -> true,
            "list"   -> List(1, 2, 3),
            "map"    -> Map("a" -> true, "b" -> false),
            "num"    -> 42,
            "numSet" -> Set(1, 2, 3),
            "str"    -> "hello",
            "strSet" -> Set("x", "y", "z")
          )
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, item))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "types-all")))
          } yield assertTrue(
            result.exists { r =>
              r.getOption[Boolean]("bool").contains(true) &&
              r.getOption[Int]("num").contains(42) &&
              r.getOption[Set[Int]]("numSet").contains(Set(1, 2, 3)) &&
              r.getOption[String]("str").contains("hello") &&
              r.getOption[Set[String]]("strSet").contains(Set("x", "y", "z")) &&
              r.map
                .get("list")
                .collect { case AttributeValue.List(v) =>
                  v.collect { case AttributeValue.Number(n) => n.intValue }.toList
                }
                .contains(List(1, 2, 3)) &&
              r.getOption[Map[String, Boolean]]("map").contains(Map("a" -> true, "b" -> false)) &&
              r.map
                .get("bin")
                .collect { case AttributeValue.Binary(b) =>
                  b.toList
                }
                .contains(Chunk.fromArray("abc".getBytes).toList)
            }
          )
        }
      },
      test("binary set values are preserved after a put/get round-trip") {
        withSingleIdKeyTable { (table, interpreter) =>
          val bytes = Chunk.fromArray("hello".getBytes)
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bs-test", "bs" -> Set(bytes))))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bs-test")))
          } yield {
            val preserved = result.flatMap(_.map.get("bs")).collect { case AttributeValue.BinarySet(v) =>
              v.map(_.toList).toSet
            }
            assertTrue(preserved.contains(Set(bytes.toList)))
          }
        }
      },
      test("null attribute is stored and retrieved") {
        withSingleIdKeyTable { (table, interpreter) =>
          val item = Item("id" -> "null-test", "nf" -> (AttributeValue.Null: AttributeValue))
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, item))
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "null-test")))
          } yield assertTrue(result.contains(item))
        }
      },
      test("empty Set is not stored as an attribute") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(
                        DynamoDBQuery.putItem(table, Item("id" -> "empty-set", "es" -> Set.empty[Int]))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "empty-set")))
          } yield assertTrue(result.flatMap(_.map.get("es")).isEmpty)
        }
      }
    )

  // ---------------------------------------------------------------------------
  // updateItem return values (UpdatedOld / AllOld / AllNew / UpdatedNew)
  // ---------------------------------------------------------------------------

  private val updateReturnValuesExtendedTests: Spec[DynamoDBEnv, Throwable] =
    suite("updateItem return values")(
      test("UpdatedOld returns only the changed attributes with their old values") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(
                        DynamoDBQuery.putItem(table, Item("id" -> "rv-a", "score" -> 0, "name" -> "alice"))
                      )
            result <- interpreter.run(
                        DynamoDBQuery
                          .updateItem(table, PrimaryKey("id" -> "rv-a"))($("score").set(42))
                          .returns(ReturnValues.UpdatedOld)
                      )
          } yield assertTrue(result.contains(Item("score" -> 0)))
        }
      },
      test("AllOld returns the entire item with its values before the update") {
        withSingleIdKeyTable { (table, interpreter) =>
          val original = Item("id" -> "rv-b", "score" -> 0, "name" -> "bob")
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, original))
            result <- interpreter.run(
                        DynamoDBQuery
                          .updateItem(table, PrimaryKey("id" -> "rv-b"))($("score").set(99))
                          .returns(ReturnValues.AllOld)
                      )
          } yield assertTrue(result.contains(original))
        }
      },
      test("AllNew returns the entire item with its values after the update") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(
                        DynamoDBQuery.putItem(table, Item("id" -> "rv-c", "score" -> 0, "name" -> "charlie"))
                      )
            result <- interpreter.run(
                        DynamoDBQuery
                          .updateItem(table, PrimaryKey("id" -> "rv-c"))($("score").set(42))
                          .returns(ReturnValues.AllNew)
                      )
          } yield assertTrue(result.contains(Item("id" -> "rv-c", "score" -> 42, "name" -> "charlie")))
        }
      },
      test("UpdatedNew returns only the changed attributes with their new values") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(
                        DynamoDBQuery.putItem(table, Item("id" -> "rv-d", "score" -> 0, "name" -> "dave"))
                      )
            result <- interpreter.run(
                        DynamoDBQuery
                          .updateItem(table, PrimaryKey("id" -> "rv-d"))($("score").set(42))
                          .returns(ReturnValues.UpdatedNew)
                      )
          } yield assertTrue(result.contains(Item("score" -> 42)))
        }
      }
    )

  // ---------------------------------------------------------------------------
  // updateItem arithmetic and setIfNotExists
  // ---------------------------------------------------------------------------

  private val updateArithmeticTests: Spec[DynamoDBEnv, Throwable] =
    suite("updateItem arithmetic and setIfNotExists")(
      test("SetAction with + increments a numeric attribute") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "arith-a", "n" -> 0)))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "arith-a")) {
                          UpdateExpression.Action.SetAction(
                            $("n"),
                            UpdateExpression.SetOperand.PathOperand($("n")) +
                              UpdateExpression.SetOperand.ValueOperand(AttributeValue.Number(5))
                          )
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "arith-a")))
          } yield assertTrue(result.contains(Item("id" -> "arith-a", "n" -> 5)))
        }
      },
      test("SetAction with - decrements a numeric attribute") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "arith-b", "n" -> 10)))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "arith-b")) {
                          UpdateExpression.Action.SetAction(
                            $("n"),
                            UpdateExpression.SetOperand.PathOperand($("n")) -
                              UpdateExpression.SetOperand.ValueOperand(AttributeValue.Number(3))
                          )
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "arith-b")))
          } yield assertTrue(result.contains(Item("id" -> "arith-b", "n" -> 7)))
        }
      },
      test("add action increments a numeric attribute") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "arith-c", "n" -> 42)))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "arith-c"))($("n").add(8))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "arith-c")))
          } yield assertTrue(result.contains(Item("id" -> "arith-c", "n" -> 50)))
        }
      },
      test("setIfNotExists sets the attribute when absent") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "ifne-a")))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "ifne-a")) {
                          $("score").setIfNotExists(99)
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "ifne-a")))
          } yield assertTrue(result.contains(Item("id" -> "ifne-a", "score" -> 99)))
        }
      },
      test("setIfNotExists leaves the attribute unchanged when already present") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "ifne-b", "score" -> 0)))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "ifne-b")) {
                          $("score").setIfNotExists(99)
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "ifne-b")))
          } yield assertTrue(result.contains(Item("id" -> "ifne-b", "score" -> 0)))
        }
      }
    )

  // ---------------------------------------------------------------------------
  // List and set update operations
  // ---------------------------------------------------------------------------

  private val listAndSetUpdateTests: Spec[DynamoDBEnv, Throwable] =
    suite("list and set update operations")(
      test("appendList extends the list at the tail") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "list-a", "xs" -> List(1))))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "list-a")) {
                          $("xs").appendList(List(2, 3))
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "list-a")))
          } yield {
            val xs = result.flatMap(_.map.get("xs")).collect { case AttributeValue.List(v) =>
              v.collect { case AttributeValue.Number(n) => n.intValue }.toList
            }
            assertTrue(xs.contains(List(1, 2, 3)))
          }
        }
      },
      test("prependList extends the list at the head") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "list-b", "xs" -> List(3))))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "list-b")) {
                          $("xs").prependList(List(1, 2))
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "list-b")))
          } yield {
            val xs = result.flatMap(_.map.get("xs")).collect { case AttributeValue.List(v) =>
              v.collect { case AttributeValue.Number(n) => n.intValue }.toList
            }
            assertTrue(xs.contains(List(1, 2, 3)))
          }
        }
      },
      test("RemoveAction removes a list element by index") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "list-c", "xs" -> List(1, 2, 3))))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "list-c")) {
                          UpdateExpression.Action.RemoveAction($("xs[1]"))
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "list-c")))
          } yield {
            val xs = result.flatMap(_.map.get("xs")).collect { case AttributeValue.List(v) =>
              v.collect { case AttributeValue.Number(n) => n.intValue }.toList
            }
            assertTrue(xs.contains(List(1, 3)))
          }
        }
      },
      test("addSet adds elements to a number set") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "set-a", "ns" -> Set(1, 2, 3))))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "set-a"))($("ns").addSet(Set(4)))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "set-a")))
          } yield {
            val ns = result.flatMap(_.getOption[Set[Int]]("ns"))
            assertTrue(ns.contains(Set(1, 2, 3, 4)))
          }
        }
      },
      test("deleteFromSet removes elements from a number set") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "set-b", "ns" -> Set(1, 2, 3))))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "set-b"))($("ns").deleteFromSet(Set(3)))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "set-b")))
          } yield {
            val ns = result.flatMap(_.getOption[Set[Int]]("ns"))
            assertTrue(ns.contains(Set(1, 2)))
          }
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Sort key range operators (< / >= / <= / between)
  // ---------------------------------------------------------------------------

  private val sortKeyRangeTests: Spec[DynamoDBEnv, Throwable] =
    suite("querySome sort key range operators")(
      test("sort key < returns items with sk strictly less than the bound") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-a", "year" -> "2021")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-a", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-a", "year" -> "2023")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "skr-a" && $("year").sortKey < "2022")
                    )
          } yield assertTrue(page.items.length == 1)
        }
      },
      test("sort key >= returns items with sk greater than or equal to the bound") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-b", "year" -> "2021")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-b", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-b", "year" -> "2023")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "skr-b" && $("year").sortKey >= "2022")
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("sort key <= returns items with sk less than or equal to the bound") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-c", "year" -> "2021")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-c", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-c", "year" -> "2023")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "skr-c" && $("year").sortKey <= "2022")
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("sort key between returns items with sk in the inclusive range") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-d", "year" -> "2021")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-d", "year" -> "2022")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-d", "year" -> "2023")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "skr-d", "year" -> "2024")))
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "skr-d" && $("year").sortKey.between("2022", "2023"))
                    )
          } yield assertTrue(page.items.length == 2)
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Condition expression variants (between / in / beginsWith / contains)
  // ---------------------------------------------------------------------------

  private val conditionExpressionTests: Spec[DynamoDBEnv, Throwable] =
    suite("condition expression variants")(
      test("between condition: deleteItem succeeds when attribute value is in range") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "cond-bt", "score" -> 50)))
            _      <- interpreter.run(
                        DynamoDBQuery
                          .deleteItem(table, PrimaryKey("id" -> "cond-bt"))
                          .where($("score").between(40, 60))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "cond-bt")))
          } yield assertTrue(result.isEmpty)
        }
      },
      test("in condition: deleteItem succeeds when attribute value is in the set") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "cond-in", "status" -> "active")))
            _      <- interpreter.run(
                        DynamoDBQuery
                          .deleteItem(table, PrimaryKey("id" -> "cond-in"))
                          .where($("status").in("active", "pending"))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "cond-in")))
          } yield assertTrue(result.isEmpty)
        }
      },
      test("beginsWith condition: deleteItem succeeds when attribute value starts with prefix") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "cond-bw", "name" -> "alice")))
            _      <- interpreter.run(
                        DynamoDBQuery
                          .deleteItem(table, PrimaryKey("id" -> "cond-bw"))
                          .where(ConditionExpression.BeginsWith($("name"), AttributeValue.String("al")))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "cond-bw")))
          } yield assertTrue(result.isEmpty)
        }
      },
      test("contains condition: deleteItem succeeds when string attribute contains the substring") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "cond-ct", "name" -> "hello world")))
            _      <- interpreter.run(
                        DynamoDBQuery
                          .deleteItem(table, PrimaryKey("id" -> "cond-ct"))
                          .where($("name").contains("hello"))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "cond-ct")))
          } yield assertTrue(result.isEmpty)
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Reserved keyword attribute names in filter/condition expressions
  // ---------------------------------------------------------------------------

  private val reservedKeywordTests: Spec[DynamoDBEnv, Throwable] =
    suite("reserved keyword attribute names")(
      test("scanSome.filter with AttributeNotExists on a DynamoDB reserved word succeeds") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "rk-1")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "rk-2")))
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "rk-3", "ttl" -> 9999)))
            page <- interpreter.run(
                      DynamoDBQuery
                        .scanSome(table, limit = 10)
                        .filter(ConditionExpression.AttributeNotExists($("ttl")))
                    )
          } yield assertTrue(page.items.length == 2)
        }
      },
      test("querySome.filter with AttributeNotExists on a DynamoDB reserved word succeeds") {
        withIdAndYearKeyTable { (table, interpreter) =>
          for {
            _    <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "rk-q", "year" -> "2022")))
            _    <- interpreter.run(
                      DynamoDBQuery.putItem(table, Item("id" -> "rk-q", "year" -> "2023", "ttl" -> 9999))
                    )
            page <- interpreter.run(
                      DynamoDBQuery
                        .querySome(table, limit = 10)
                        .whereKey($("id").partitionKey === "rk-q")
                        .filter(ConditionExpression.AttributeNotExists($("ttl")))
                    )
          } yield assertTrue(page.items.length == 1)
        }
      },
      test("putItem.where with AttributeNotExists on a DynamoDB reserved word succeeds") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "rk-p", "score" -> 0)))
            _      <- interpreter.run(
                        DynamoDBQuery
                          .putItem(table, Item("id" -> "rk-p", "score" -> 1))
                          .where(ConditionExpression.AttributeNotExists($("ttl")))
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "rk-p")))
          } yield assertTrue(result.contains(Item("id" -> "rk-p", "score" -> 1)))
        }
      }
    )

  // ---------------------------------------------------------------------------
  // Projection expression edge cases
  // ---------------------------------------------------------------------------
  // TODO: streaming operations (scanAll / queryAll / parallel scan) are not
  // available in the new LL API; add tests here when they are implemented.

  private val projectionTests: Spec[DynamoDBEnv, Throwable] =
    suite("projection expression edge cases")(
      test("nested map field projection returns only the nested value") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(
                        DynamoDBQuery.putItem(
                          table,
                          Item("id" -> "pe-nm", "stats" -> Map("wins" -> 10, "losses" -> 5))
                        )
                      )
            result <- interpreter.run(
                        DynamoDBQuery.getItem(table, PrimaryKey("id" -> "pe-nm"), $("stats.wins"))
                      )
          } yield assertTrue(result.contains(Item("stats" -> Map("wins" -> 10))))
        }
      },
      test("backtick-quoted field name containing a dot is projected correctly") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "pe-bt", "foo.bar" -> "value")))
            result <- interpreter.run(
                        DynamoDBQuery.getItem(table, PrimaryKey("id" -> "pe-bt"), $("`foo.bar`"))
                      )
          } yield assertTrue(result.contains(Item("foo.bar" -> "value")))
        }
      },
      test("hyphen in field name is projected correctly") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "pe-hy", "foo-bar" -> "value")))
            result <- interpreter.run(
                        DynamoDBQuery.getItem(table, PrimaryKey("id" -> "pe-hy"), $("foo-bar"))
                      )
          } yield assertTrue(result.contains(Item("foo-bar" -> "value")))
        }
      },
      test("backtick-quoted field name works in condition expression") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bt-ce", "foo.bar" -> "original")))
            _      <- interpreter.run(
                        DynamoDBQuery
                          .putItem(table, Item("id" -> "bt-ce", "foo.bar" -> "updated"))
                          .where($("`foo.bar`") === "original")
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bt-ce")))
          } yield assertTrue(result.contains(Item("id" -> "bt-ce", "foo.bar" -> "updated")))
        }
      },
      test("backtick-quoted field name works in update expression") {
        withSingleIdKeyTable { (table, interpreter) =>
          for {
            _      <- interpreter.run(DynamoDBQuery.putItem(table, Item("id" -> "bt-ue", "foo.bar" -> "before")))
            _      <- interpreter.run(
                        DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "bt-ue")) {
                          $("`foo.bar`").set("after")
                        }
                      )
            result <- interpreter.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bt-ue")))
          } yield assertTrue(result.contains(Item("id" -> "bt-ue", "foo.bar" -> "after")))
        }
      }
    )

  def spec = suite("DynamoDB CRUD")(
    suite("ZIO interpreter")(
      singleKeyTests,
      compositeKeyTests,
      batchingTests,
      batchGetItemTests,
      updateItemTests,
      conditionAndFilterTests,
      querySomeTests,
      capacityTests,
      consistencyTests,
      returnsTests,
      startKeyTests,
      segmentTests,
      selectTests,
      gsiTests,
      lsiTests,
      allAttributeTypesTests,
      updateReturnValuesExtendedTests,
      updateArithmeticTests,
      listAndSetUpdateTests,
      sortKeyRangeTests,
      conditionExpressionTests,
      reservedKeywordTests,
      projectionTests
    )
      .provideSome[DynamoDbAsyncClient](zioEnvLayer),
    suite("CE interpreter")(
      singleKeyTests,
      compositeKeyTests,
      batchingTests,
      batchGetItemTests,
      updateItemTests,
      conditionAndFilterTests,
      querySomeTests,
      capacityTests,
      consistencyTests,
      returnsTests,
      startKeyTests,
      segmentTests,
      selectTests,
      gsiTests,
      lsiTests,
      allAttributeTypesTests,
      updateReturnValuesExtendedTests,
      updateArithmeticTests,
      listAndSetUpdateTests,
      sortKeyRangeTests,
      conditionExpressionTests,
      reservedKeywordTests,
      projectionTests
    )
      .provideSome[DynamoDbAsyncClient](ceEnvLayer),
    suite("Future interpreter")(
      singleKeyTests,
      compositeKeyTests,
      batchingTests,
      batchGetItemTests,
      updateItemTests,
      conditionAndFilterTests,
      querySomeTests,
      capacityTests,
      consistencyTests,
      returnsTests,
      startKeyTests,
      segmentTests,
      selectTests,
      gsiTests,
      lsiTests,
      allAttributeTypesTests,
      updateReturnValuesExtendedTests,
      updateArithmeticTests,
      listAndSetUpdateTests,
      sortKeyRangeTests,
      conditionExpressionTests,
      reservedKeywordTests,
      projectionTests
    )
      .provideSome[DynamoDbAsyncClient](futureEnvLayer)
  ) @@ TestAspect.sequential
}
