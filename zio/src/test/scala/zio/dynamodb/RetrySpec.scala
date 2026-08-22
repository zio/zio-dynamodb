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

import zio._
import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError
import zio.test._
import zio.test.Assertion.{ anything, equalTo, hasField, isSubtype }
import zio.test.TestClock

import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }

object RetrySpec extends ZIOSpecDefault {

  // Minimal AwsInterpreter[Task] backed by a per-test Ref so individual
  // tests can control what runBatchWriteItem / runBatchGetItem return.
  private def makeInterp(
    batchWriteResponses: List[DynamoDBQuery.BatchWriteItem.Response] = Nil,
    batchGetResponses: List[DynamoDBQuery.BatchGetItem.Response] = Nil,
    getItemEffect: Task[Option[Item]] = ZIO.succeed(None),
    putItemEffect: Task[Option[Item]] = ZIO.succeed(None),
    batchWriteItemEffect: Option[Task[DynamoDBQuery.BatchWriteItem.Response]] = None,
    batchGetItemEffect: Option[Task[DynamoDBQuery.BatchGetItem.Response]] = None
  ): ZIO[Any, Nothing, AwsInterpreter[Task]] =
    for {
      writeRef <- Ref.make(batchWriteResponses)
      getRef   <- Ref.make(batchGetResponses)
    } yield new AwsInterpreter[Task] {
      private[dynamodb] def pure[A](a: A): Task[A]                               = ZIO.succeed(a)
      private[dynamodb] def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.map(f)
      private[dynamodb] def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)
      protected def product[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]        = fa.zip(fb)
      protected def productPar[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]     = fa.zipPar(fb)
      protected def fail[A](e: DynamoDBError): Task[A]                           = ZIO.fail(e)
      protected def absolve[A](fa: Task[Either[ItemError, A]]): Task[A]          =
        fa.flatMap(ZIO.fromEither(_))

      private[dynamodb] def sleep(d: FiniteDuration): Task[Unit]                =
        ZIO.sleep(zio.Duration.fromScala(d))
      private[dynamodb] def attempt[A](fa: Task[A]): Task[Either[Throwable, A]] = fa.either
      private[dynamodb] def raiseError[A](t: Throwable): Task[A]                = ZIO.fail(t)

      protected def runGetItem(q: DynamoDBQuery.GetItem): Task[Option[Item]]                          = getItemEffect
      protected def runPutItem(q: DynamoDBQuery.PutItem): Task[Option[Item]]                          = putItemEffect
      protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): Task[Option[Item]]                    = ZIO.succeed(None)
      protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): Task[Option[Item]]                    = ZIO.succeed(None)
      protected def runQuery(q: DynamoDBQuery.Query): Task[Page[Item]]                                =
        ZIO.succeed(Page(Chunk.empty, None, 0, 0))
      protected def runScan(q: DynamoDBQuery.Scan): Task[Page[Item]]                                  =
        ZIO.succeed(Page(Chunk.empty, None, 0, 0))
      protected def runCreateTable(q: DynamoDBQuery.CreateTable): Task[Unit]                          = ZIO.succeed(())
      protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): Task[Unit]                          = ZIO.succeed(())
      protected def runDescribeTable(
        q: DynamoDBQuery.DescribeTable
      ): Task[DynamoDBQuery.DescribeTableResponse]                                                    =
        ZIO.succeed(
          DynamoDBQuery.DescribeTableResponse(
            "arn:stub",
            DynamoDBQuery.TableStatus.Active,
            0L,
            0L
          )
        )
      protected def runBatchGetItem(
        q: DynamoDBQuery.BatchGetItem
      ): Task[DynamoDBQuery.BatchGetItem.Response]                                                    =
        batchGetItemEffect.getOrElse(
          getRef.modify {
            case head :: tail => (head, tail)
            case Nil          => (DynamoDBQuery.BatchGetItem.Response(), Nil)
          }
        )
      protected def runBatchWriteItem(
        q: DynamoDBQuery.BatchWriteItem
      ): Task[DynamoDBQuery.BatchWriteItem.Response]                                                  =
        batchWriteItemEffect.getOrElse(
          writeRef.modify {
            case head :: tail => (head, tail)
            case Nil          => (DynamoDBQuery.BatchWriteItem.Response(None), Nil)
          }
        )
      protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): Task[Chunk[Option[Item]]] =
        ZIO.succeed(Chunk.fill(q.getItems.length)(None))
      protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): Task[Unit]            =
        ZIO.unit
    }

  def spec = suite("RetrySpec")(
    suite("withRetry — effect-level")(
      test("NoRetry does not retry on failure") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp()
          result <- interp
                      .withRetry(RetryPolicy.NoRetry, _ => true) {
                        calls.updateAndGet(_ + 1) *> ZIO.fail(new RuntimeException("boom"))
                      }
                      .exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },

      test("succeeds immediately without retrying when effect succeeds") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp()
          result <- interp.withRetry(
                      RetryPolicy.ExponentialBackoff(3, FiniteDuration(100, MILLISECONDS), jitter = false),
                      _ => true
                    ) {
                      calls.updateAndGet(_ + 1).as("ok")
                    }
          n      <- calls.get
        } yield assertTrue(result == "ok" && n == 1)
      },

      test("does not retry non-retryable errors") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp()
          result <- interp
                      .withRetry(
                        RetryPolicy.ExponentialBackoff(3, FiniteDuration(100, MILLISECONDS), jitter = false),
                        _ => false // nothing is retryable
                      ) {
                        calls.updateAndGet(_ + 1) *> ZIO.fail(new RuntimeException("fatal"))
                      }
                      .exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },

      test("ExponentialBackoff retries and succeeds — TestClock controls time") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp()
          // Fails twice then succeeds; jitter=false → delays are 100ms, 200ms
          fiber  <- interp
                      .withRetry(
                        RetryPolicy.ExponentialBackoff(
                          maxRetries = 3,
                          initialDelay = FiniteDuration(100, MILLISECONDS),
                          jitter = false
                        ),
                        _ => true
                      ) {
                        calls.updateAndGet(_ + 1).flatMap { n =>
                          if (n < 3) ZIO.fail(new RuntimeException("throttled"))
                          else ZIO.succeed("done")
                        }
                      }
                      .fork
          _      <- TestClock.adjust(100.millis)
          _      <- TestClock.adjust(200.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result == "done" && n == 3)
      },

      test("fatal error is re-raised immediately without retrying") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp()
          result <- interp
                      .withRetry(
                        RetryPolicy.ExponentialBackoff(
                          maxRetries = 3,
                          initialDelay = FiniteDuration(50, MILLISECONDS),
                          jitter = false
                        ),
                        _ => true // would retry anything — but NonFatal gate fires first
                      ) {
                        calls.updateAndGet(_ + 1) *> ZIO.fail(new OutOfMemoryError("heap space"))
                      }
                      .exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },

      test("ExponentialBackoff exhausts retries and re-raises last error") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp()
          fiber  <- interp
                      .withRetry(
                        RetryPolicy.ExponentialBackoff(
                          maxRetries = 2,
                          initialDelay = FiniteDuration(50, MILLISECONDS),
                          jitter = false
                        ),
                        _ => true
                      ) {
                        calls.updateAndGet(_ + 1) *> ZIO.fail(new RuntimeException("always fails"))
                      }
                      .exit
                      .fork
          _      <- TestClock.adjust(50.millis)
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 3)
      }
    ),

    suite("BatchWriteItem — response-level retry (via interp.run)")(
      test("returns Complete when no unprocessed items") {
        for {
          interp <- makeInterp(
                      batchWriteResponses = List(DynamoDBQuery.BatchWriteItem.Response(None))
                    )
          result <- interp.run(
                      DynamoDBQuery.batchWriteItem(List(Item("id" -> "a")))(i => DynamoDBQuery.putItem("t", i))
                    )
        } yield assert(result)(isSubtype[Batch.WriteResult.Complete](anything))
      },

      test("returns Complete after retrying unprocessed items — TestClock controls time") {
        val item        = Item("id" -> "a")
        val unprocessed = Some(
          MapOfSet.empty[String, DynamoDBQuery.BatchWriteItem.Write] +
            ("t" -> DynamoDBQuery.BatchWriteItem.Put(item))
        )
        for {
          interp <- makeInterp(
                      batchWriteResponses = List(
                        DynamoDBQuery.BatchWriteItem.Response(unprocessed),
                        DynamoDBQuery.BatchWriteItem.Response(None)
                      )
                    )
          fiber  <- interp
                      .run(
                        DynamoDBQuery
                          .batchWriteItem(List(item))(i => DynamoDBQuery.putItem("t", i))
                          .withRetryPolicy(
                            RetryPolicy.ExponentialBackoff(
                              maxRetries = 2,
                              initialDelay = FiniteDuration(100, MILLISECONDS),
                              jitter = false
                            )
                          )
                      )
                      .fork
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
        } yield assert(result)(isSubtype[Batch.WriteResult.Complete](anything))
      },

      test("returns Incomplete when response-level policy exhausted") {
        val item        = Item("id" -> "a")
        val unprocessed = Some(
          MapOfSet.empty[String, DynamoDBQuery.BatchWriteItem.Write] +
            ("t" -> DynamoDBQuery.BatchWriteItem.Put(item))
        )
        for {
          interp <- makeInterp(
                      batchWriteResponses = List(
                        DynamoDBQuery.BatchWriteItem.Response(unprocessed),
                        DynamoDBQuery.BatchWriteItem.Response(unprocessed)
                      )
                    )
          fiber  <- interp
                      .run(
                        DynamoDBQuery
                          .batchWriteItem(List(item))(i => DynamoDBQuery.putItem("t", i))
                          .withRetryPolicy(
                            RetryPolicy.ExponentialBackoff(
                              maxRetries = 1,
                              initialDelay = FiniteDuration(100, MILLISECONDS),
                              jitter = false
                            )
                          )
                      )
                      .fork
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
        } yield assert(result)(isSubtype[Batch.WriteResult.Incomplete](anything))
      },

      test("fatal error propagates as a failed effect, not WriteResult.Failed") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      batchWriteItemEffect = Some(
                        calls.updateAndGet(_ + 1) *>
                          ZIO.fail(new OutOfMemoryError("heap space"))
                      )
                    )
          result <- interp
                      .run(
                        DynamoDBQuery
                          .batchWriteItem(List(Item("id" -> "a")))(i => DynamoDBQuery.putItem("t", i))
                          .withRetryPolicy(
                            RetryPolicy.ExponentialBackoff(
                              maxRetries = 3,
                              initialDelay = FiniteDuration(50, MILLISECONDS),
                              jitter = false
                            )
                          )
                      )
                      .exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },

      test("non-retryable error fails immediately — effectRetries and responseRetries are both 0") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      batchWriteItemEffect = Some(
                        calls.updateAndGet(_ + 1) *>
                          ZIO.fail(new RuntimeException("ValidationException: invalid attribute"))
                      )
                    )
          result <- interp.run(
                      DynamoDBQuery
                        .batchWriteItem(List(Item("id" -> "a")))(i => DynamoDBQuery.putItem("t", i))
                        .withRetryPolicy(
                          RetryPolicy.ExponentialBackoff(
                            maxRetries = 3,
                            initialDelay = FiniteDuration(50, MILLISECONDS),
                            jitter = false
                          )
                        )
                    )
          n      <- calls.get
        } yield result match {
          case Batch.WriteResult.Failed(_, responseRetries, effectRetries) =>
            assertTrue(
              n == 1 &&
                responseRetries == 0 &&
                effectRetries == 0
            )
          case _                                                           =>
            assertTrue(false)
        }
      },

      test("returns Failed with cause when effect-level retries exhausted") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      batchWriteItemEffect = Some(
                        calls.updateAndGet(_ + 1) *>
                          ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                      )
                    )
          fiber  <- interp
                      .run(
                        DynamoDBQuery
                          .batchWriteItem(List(Item("id" -> "a")))(i => DynamoDBQuery.putItem("t", i))
                          .withRetryPolicy(
                            RetryPolicy.ExponentialBackoff(
                              maxRetries = 2,
                              initialDelay = FiniteDuration(50, MILLISECONDS),
                              jitter = false
                            )
                          )
                      )
                      .fork
          _      <- TestClock.adjust(50.millis)
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
          n      <- calls.get
        } yield result match {
          case Batch.WriteResult.Failed(cause, responseRetries, effectRetries) =>
            assertTrue(
              cause.getMessage.contains("ProvisionedThroughputExceededException") &&
                n == 3 &&               // 3 effect-level calls via withRetryTracked
                responseRetries == 0 && // failed on the first batch submission (no response-level retries)
                effectRetries == 2      // 2 retries after the initial attempt
            )
          case _                                                               =>
            assertTrue(false)
        }
      }
    ),

    suite("withRetry — DynamoDBQuery operations")(
      test("getItem succeeds on first attempt — no retry") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1).as(Some(Item("id" -> "alice")))
                    )
          result <- interp.withRetry(
                      RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false),
                      RetryPolicy.isRetryable
                    )(interp.run(DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice"))))
          n      <- calls.get
        } yield assertTrue(result.contains(Item("id" -> "alice")) && n == 1)
      },

      test("getItem retries on ProvisionedThroughputExceededException then succeeds") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                        if (n < 2)
                          ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                        else
                          ZIO.succeed(Some(Item("id" -> "alice")))
                      }
                    )
          fiber  <- interp
                      .withRetry(
                        RetryPolicy.ExponentialBackoff(
                          maxRetries = 3,
                          initialDelay = FiniteDuration(50, MILLISECONDS),
                          jitter = false
                        ),
                        RetryPolicy.isRetryable
                      )(interp.run(DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice"))))
                      .fork
          _      <- TestClock.adjust(50.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.contains(Item("id" -> "alice")) && n == 2)
      },

      test("putItem does not retry a non-retryable error") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      putItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ValidationException"))
                    )
          result <- interp
                      .withRetry(
                        RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false),
                        RetryPolicy.isRetryable
                      )(interp.run(DynamoDBQuery.putItem("t", Item("id" -> "bob"))))
                      .exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },

      test("getItem exhausts retries on persistent ThrottlingException") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ThrottlingException"))
                    )
          fiber  <- interp
                      .withRetry(
                        RetryPolicy.ExponentialBackoff(
                          maxRetries = 2,
                          initialDelay = FiniteDuration(50, MILLISECONDS),
                          jitter = false
                        ),
                        RetryPolicy.isRetryable
                      )(interp.run(DynamoDBQuery.getItem("t", PrimaryKey("id" -> "x"))))
                      .exit
                      .fork
          _      <- TestClock.adjust(50.millis)
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 3)
      }
    ),

    suite("withRetryPolicy — embedded in query")(
      test("getItem with retryPolicy retries on ProvisionedThroughputExceededException then succeeds") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                        if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                        else ZIO.succeed(Some(Item("id" -> "alice")))
                      }
                    )
          query =
            DynamoDBQuery
              .getItem("t", PrimaryKey("id" -> "alice"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber  <- interp.run(query).fork
          _      <- TestClock.adjust(50.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.contains(Item("id" -> "alice")) && n == 2)
      },

      test("getItem with retryPolicy exhausts retries and fails") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ThrottlingException"))
                    )
          query =
            DynamoDBQuery
              .getItem("t", PrimaryKey("id" -> "x"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(2, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber  <- interp.run(query).exit.fork
          _      <- TestClock.adjust(50.millis)
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 3)
      },

      test("getItem without retryPolicy does not retry") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                    )
          result <- interp.run(DynamoDBQuery.getItem("t", PrimaryKey("id" -> "x"))).exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },

      test("zipPar propagates retryPolicy to both branches independently") {
        for {
          getCount <- Ref.make(0)
          putCount <- Ref.make(0)
          interp   <- makeInterp(
                        getItemEffect = getCount.updateAndGet(_ + 1).flatMap { n =>
                          if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                          else ZIO.succeed(Some(Item("id" -> "alice")))
                        },
                        putItemEffect = putCount.updateAndGet(_ + 1).flatMap { n =>
                          if (n < 2) ZIO.fail(new RuntimeException("ThrottlingException"))
                          else ZIO.succeed(None)
                        }
                      )
          policy = RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false)
          query = DynamoDBQuery
                    .getItem("t", PrimaryKey("id" -> "alice"))
                    .zipPar(DynamoDBQuery.putItem("t", Item("id" -> "alice")))
                    .withRetryPolicy(policy)
          fiber    <- interp.run(query).fork
          _        <- TestClock.adjust(50.millis)
          result   <- fiber.join
          gc       <- getCount.get
          pc       <- putCount.get
        } yield assertTrue(gc == 2 && pc == 2 && result._1.contains(Item("id" -> "alice")))
      }
    ),

    suite("BatchGetItem — response-level retry (via interp.run)")(
      test("returns Complete when no unprocessed keys") {
        for {
          interp <- makeInterp(
                      batchGetResponses = List(DynamoDBQuery.BatchGetItem.Response())
                    )
          result <- interp.run(
                      DynamoDBQuery.batchGetItem(List("a"))(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id)))
                    )
        } yield assert(result)(isSubtype[Batch.GetResult.Complete](anything))
      },

      test("fatal error propagates as a failed effect, not GetResult.Failed") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      batchGetItemEffect = Some(
                        calls.updateAndGet(_ + 1) *>
                          ZIO.fail(new OutOfMemoryError("heap space"))
                      )
                    )
          result <- interp
                      .run(
                        DynamoDBQuery
                          .batchGetItem(List("a"))(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id)))
                          .withRetryPolicy(
                            RetryPolicy.ExponentialBackoff(
                              maxRetries = 3,
                              initialDelay = FiniteDuration(50, MILLISECONDS),
                              jitter = false
                            )
                          )
                      )
                      .exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },

      test("non-retryable error fails immediately — effectRetries and responseRetries are both 0") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      batchGetItemEffect = Some(
                        calls.updateAndGet(_ + 1) *>
                          ZIO.fail(new RuntimeException("ValidationException: invalid key"))
                      )
                    )
          result <- interp.run(
                      DynamoDBQuery
                        .batchGetItem(List("a"))(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id)))
                        .withRetryPolicy(
                          RetryPolicy.ExponentialBackoff(
                            maxRetries = 3,
                            initialDelay = FiniteDuration(50, MILLISECONDS),
                            jitter = false
                          )
                        )
                    )
          n      <- calls.get
        } yield result match {
          case Batch.GetResult.Failed(_, responseRetries, effectRetries) =>
            assertTrue(
              n == 1 &&
                responseRetries == 0 &&
                effectRetries == 0
            )
          case _                                                         =>
            assertTrue(false)
        }
      },

      test("returns Complete after retrying unprocessed keys — TestClock controls time") {
        import scala.collection.immutable.{ Map => ScalaMap }
        val unprocessedKeys = ScalaMap(
          "t" -> DynamoDBQuery.BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("id" -> "a")),
            projectionExpressionSet = Set.empty
          )
        )
        for {
          interp <- makeInterp(
                      batchGetResponses = List(
                        DynamoDBQuery.BatchGetItem.Response(unprocessedKeys = unprocessedKeys),
                        DynamoDBQuery.BatchGetItem.Response()
                      )
                    )
          fiber  <- interp
                      .run(
                        DynamoDBQuery
                          .batchGetItem(List("a"))(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id)))
                          .withRetryPolicy(
                            RetryPolicy.ExponentialBackoff(
                              maxRetries = 2,
                              initialDelay = FiniteDuration(100, MILLISECONDS),
                              jitter = false
                            )
                          )
                      )
                      .fork
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
        } yield assert(result)(isSubtype[Batch.GetResult.Complete](anything))
      },

      test("items fetched in an earlier attempt are not lost after retrying the residual keys") {
        import scala.collection.immutable.{ Map => ScalaMap }
        val itemA           = Item("id" -> "a")
        val itemB           = Item("id" -> "b")
        val unprocessedKeys = ScalaMap(
          "t" -> DynamoDBQuery.BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("id" -> "b")),
            projectionExpressionSet = Set.empty
          )
        )
        for {
          interp <- makeInterp(
                      batchGetResponses = List(
                        DynamoDBQuery.BatchGetItem.Response(
                          responses = MapOfSet.empty[String, Item] + ("t" -> itemA),
                          unprocessedKeys = unprocessedKeys
                        ),
                        DynamoDBQuery.BatchGetItem.Response(
                          responses = MapOfSet.empty[String, Item] + ("t" -> itemB)
                        )
                      )
                    )
          fiber  <- interp
                      .run(
                        DynamoDBQuery
                          .batchGetItem(List("a", "b"))(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id)))
                          .withRetryPolicy(
                            RetryPolicy.ExponentialBackoff(
                              maxRetries = 2,
                              initialDelay = FiniteDuration(100, MILLISECONDS),
                              jitter = false
                            )
                          )
                      )
                      .fork
          _      <- TestClock.adjust(100.millis)
          result <- fiber.join
        } yield assert(result)(
          isSubtype[Batch.GetResult.Complete](
            hasField("responses", _.response.responses.getOrElse("t", Set.empty), equalTo(Set(itemA, itemB)))
          )
        )
      }
    )
  )
}
