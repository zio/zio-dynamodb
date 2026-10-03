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
    updateItemEffect: Task[Option[Item]] = ZIO.succeed(None),
    queryEffect: Task[Page[Item]] = ZIO.succeed(Page(Chunk.empty, None, 0, 0)),
    scanEffect: Task[Page[Item]] = ZIO.succeed(Page(Chunk.empty, None, 0, 0)),
    batchWriteItemEffect: Option[Task[DynamoDBQuery.BatchWriteItem.Response]] = None,
    batchGetItemEffect: Option[Task[DynamoDBQuery.BatchGetItem.Response]] = None,
    defaultRetryPolicyParam: Option[EffectfulRetryPolicy[Task]] = None,
    retryInterceptorParam: Option[RetryInterceptor[Task]] = None,
    batchRetryInterceptorParam: Option[BatchRetryInterceptor[Task]] = None,
    retryQuotaParam: Option[RetryQuota[Task]] = None
  ): ZIO[Any, Nothing, AwsInterpreter[Task]] =
    for {
      writeRef <- Ref.make(batchWriteResponses)
      getRef   <- Ref.make(batchGetResponses)
    } yield new AwsInterpreter[Task] {
      override protected def defaultRetryPolicy: Option[EffectfulRetryPolicy[Task]]     = defaultRetryPolicyParam
      override protected def retryInterceptor: Option[RetryInterceptor[Task]]           = retryInterceptorParam
      override protected def batchRetryInterceptor: Option[BatchRetryInterceptor[Task]] =
        batchRetryInterceptorParam
      override protected def retryQuota: Option[RetryQuota[Task]]                       = retryQuotaParam
      private[dynamodb] def pure[A](a: A): Task[A]                                      = ZIO.succeed(a)
      private[dynamodb] def map[A, B](fa: Task[A])(f: A => B): Task[B]                  = fa.map(f)
      private[dynamodb] def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B]        = fa.flatMap(f)
      protected def product[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]               = fa.zip(fb)
      protected def productPar[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]            = fa.zipPar(fb)
      protected def fail[A](e: DynamoDBError): Task[A]                                  = ZIO.fail(e)
      protected def absolve[A](fa: Task[Either[ItemError, A]]): Task[A]                 =
        fa.flatMap(ZIO.fromEither(_))

      private[dynamodb] def sleep(d: FiniteDuration): Task[Unit]                =
        ZIO.sleep(zio.Duration.fromScala(d))
      private[dynamodb] def attempt[A](fa: Task[A]): Task[Either[Throwable, A]] = fa.either
      private[dynamodb] def raiseError[A](t: Throwable): Task[A]                = ZIO.fail(t)

      protected def runGetItem(q: DynamoDBQuery.GetItem): Task[Option[Item]]                          = getItemEffect
      protected def runPutItem(q: DynamoDBQuery.PutItem): Task[Option[Item]]                          = putItemEffect
      protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): Task[Option[Item]]                    = updateItemEffect
      protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): Task[Option[Item]]                    = ZIO.succeed(None)
      protected def runQuery(q: DynamoDBQuery.Query): Task[Page[Item]]                                = queryEffect
      protected def runScan(q: DynamoDBQuery.Scan): Task[Page[Item]]                                  = scanEffect
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
      },

      test("a reused F[A] value gets a fresh Attempt on each execution, not one shared across all of them") {
        // Regression test: newAttempt() must be deferred into the effect, not evaluated once
        // when withRetry(...) is called — otherwise every re-run of a captured effect value
        // (e.g. `val effect = interp.run(query)`, run more than once) shares one Attempt,
        // breaking stateful policies built via RetryPolicy.statefulCustom.
        val onceOnlyPolicy = RetryPolicy.statefulCustom { () =>
          var used = false
          (_: Int) => if (used) None else { used = true; Some(FiniteDuration(1, MILLISECONDS)) }
        }
        for {
          calls   <- Ref.make(0)
          interp  <- makeInterp()
          effect = interp
                     .withRetry(onceOnlyPolicy, RetryPolicy.isRetryable) {
                       calls.updateAndGet(_ + 1).flatMap { n =>
                         if (n % 2 == 1) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                         else ZIO.succeed(n)
                       }
                     }
                     .exit
          fiber1  <- effect.fork
          _       <- TestClock.adjust(1.millis)
          result1 <- fiber1.join
          fiber2  <- effect.fork // same captured value, executed a second time
          _       <- TestClock.adjust(1.millis)
          result2 <- fiber2.join
          n       <- calls.get
        } yield assertTrue(result1.isSuccess && result2.isSuccess && n == 4)
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
          Map("t" -> Chunk[DynamoDBQuery.BatchWriteItem.Write](DynamoDBQuery.BatchWriteItem.Put(item)))
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
          Map("t" -> Chunk[DynamoDBQuery.BatchWriteItem.Write](DynamoDBQuery.BatchWriteItem.Put(item)))
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
      test("updateItem retries when it opts in with an explicit retryPolicy") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      updateItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                        if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                        else ZIO.succeed(Some(Item("id" -> "alice")))
                      }
                    )
          query =
            DynamoDBQuery
              .updateItem("t", PrimaryKey("id" -> "alice"))(ProjectionExpression.$("name").set("bob"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber  <- interp.run(query).fork
          _      <- TestClock.adjust(50.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.contains(Item("id" -> "alice")) && n == 2)
      }
    ),

    suite("defaultRetryPolicy — interpreter-level fallback")(
      test("getItem without its own retryPolicy falls back to defaultRetryPolicy") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                        if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                        else ZIO.succeed(Some(Item("id" -> "alice")))
                      },
                      defaultRetryPolicyParam =
                        Some(ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(3)))
                    )
          fiber  <- interp.run(DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice"))).fork
          _      <- TestClock.adjust(50.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.contains(Item("id" -> "alice")) && n == 2)
      },
      test("a query's own retryPolicy takes precedence over defaultRetryPolicy") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException")),
                      defaultRetryPolicyParam =
                        Some(ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(3)))
                    )
          query =
            DynamoDBQuery.getItem("t", PrimaryKey("id" -> "x")).withRetryPolicy(RetryPolicy.NoRetry)
          result <- interp.run(query).exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1) // NoRetry wins — defaultRetryPolicy never consulted
      },
      test("query without its own retryPolicy falls back to defaultRetryPolicy") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      queryEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                        if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                        else ZIO.succeed(Page(Chunk(Item("id" -> "alice")), None, 0, 0))
                      },
                      defaultRetryPolicyParam =
                        Some(ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(3)))
                    )
          query = DynamoDBQuery.Query("t", limit = 10).whereKey(ProjectionExpression.$("id").partitionKey === "alice")
          fiber  <- interp.run(query).fork
          _      <- TestClock.adjust(50.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.items.contains(Item("id" -> "alice")) && n == 2)
      },
      test("scan without its own retryPolicy falls back to defaultRetryPolicy") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      scanEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                        if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                        else ZIO.succeed(Page(Chunk(Item("id" -> "alice")), None, 0, 0))
                      },
                      defaultRetryPolicyParam =
                        Some(ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(3)))
                    )
          fiber  <- interp.run(DynamoDBQuery.scan("t", limit = 10)).fork
          _      <- TestClock.adjust(50.millis)
          result <- fiber.join
          n      <- calls.get
        } yield assertTrue(result.items.contains(Item("id" -> "alice")) && n == 2)
      },
      test(
        "updateItem does NOT fall back to defaultRetryPolicy — its Action DSL can be non-idempotent (.add/.increment/.decrement/.appendList)"
      ) {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      updateItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException")),
                      defaultRetryPolicyParam =
                        Some(ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(3)))
                    )
          query = DynamoDBQuery.updateItem("t", PrimaryKey("id" -> "x"))(ProjectionExpression.$("count").increment(1))
          result <- interp.run(query).exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1) // defaultRetryPolicy never consulted for UpdateItem
      },
      test("BatchGetItem without its own retryPolicy resubmits unprocessed keys via defaultRetryPolicy") {
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
                      ),
                      defaultRetryPolicyParam =
                        Some(ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(2)))
                    )
          fiber  <-
            interp
              .run(DynamoDBQuery.batchGetItem(List("a"))(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id))))
              .fork
          _      <- TestClock.adjust(50.millis)
          result <- fiber.join
        } yield assert(result)(isSubtype[Batch.GetResult.Complete](anything))
      }
    ),

    suite("RetryInterceptor / BatchRetryInterceptor")(
      test("onRetry fires once per retried attempt with correct metadata, error, and attempt number") {
        for {
          calls    <- Ref.make(0)
          retryLog <- Ref.make(Chunk.empty[(DynamoDBRetryMetadata, String, Int)])
          retryInterceptor = new RetryInterceptor[Task] {
                               def onRetry(meta: DynamoDBRetryMetadata, error: Throwable, attempt: Int): Task[Unit] =
                                 retryLog.update(_ :+ ((meta, error.getMessage, attempt)))
                             }
          interp   <- makeInterp(
                        getItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                          if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                          else ZIO.succeed(Some(Item("id" -> "alice")))
                        },
                        retryInterceptorParam = Some(retryInterceptor)
                      )
          query =
            DynamoDBQuery
              .getItem("t", PrimaryKey("id" -> "alice"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber    <- interp.run(query).fork
          _        <- TestClock.adjust(50.millis)
          result   <- fiber.join
          log      <- retryLog.get
        } yield assertTrue(
          result.contains(Item("id" -> "alice")),
          log == Chunk(
            (
              DynamoDBRetryMetadata.GetItem("t", CorrelationContext(Some(PrimaryKey("id" -> "alice")))),
              "ProvisionedThroughputExceededException",
              0
            )
          )
        )
      },
      test("onRetry also fires for UpdateItem when it opts in with an explicit retryPolicy") {
        for {
          calls    <- Ref.make(0)
          retryLog <- Ref.make(Chunk.empty[DynamoDBRetryMetadata])
          retryInterceptor = new RetryInterceptor[Task] {
                               def onRetry(meta: DynamoDBRetryMetadata, error: Throwable, attempt: Int): Task[Unit] =
                                 retryLog.update(_ :+ meta)
                             }
          interp   <- makeInterp(
                        updateItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                          if (n < 2) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                          else ZIO.succeed(Some(Item("id" -> "alice")))
                        },
                        retryInterceptorParam = Some(retryInterceptor)
                      )
          query =
            DynamoDBQuery
              .updateItem("t", PrimaryKey("id" -> "alice"))(ProjectionExpression.$("name").set("bob"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber    <- interp.run(query).fork
          _        <- TestClock.adjust(50.millis)
          result   <- fiber.join
          log      <- retryLog.get
        } yield assertTrue(
          result.contains(Item("id" -> "alice")),
          log == Chunk(DynamoDBRetryMetadata.UpdateItem("t", CorrelationContext(Some(PrimaryKey("id" -> "alice")))))
        )
      },
      test("onRetry is never called when the effect succeeds on the first attempt") {
        for {
          retryLog <- Ref.make(Chunk.empty[DynamoDBRetryMetadata])
          retryInterceptor = new RetryInterceptor[Task] {
                               def onRetry(meta: DynamoDBRetryMetadata, error: Throwable, attempt: Int): Task[Unit] =
                                 retryLog.update(_ :+ meta)
                             }
          interp   <- makeInterp(
                        getItemEffect = ZIO.succeed(Some(Item("id" -> "alice"))),
                        retryInterceptorParam = Some(retryInterceptor)
                      )
          query = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice")).withRetryPolicy(RetryPolicy.NoRetry)
          _        <- interp.run(query)
          log      <- retryLog.get
        } yield assertTrue(log.isEmpty)
      },
      test("onBatchGetRetry fires with unprocessed keys reshaped to Map[String, Set[PrimaryKey]]") {
        import scala.collection.immutable.{ Map => ScalaMap }
        val unprocessedKeys = ScalaMap(
          "t" -> DynamoDBQuery.BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("id" -> "a")),
            projectionExpressionSet = Set.empty
          )
        )
        for {
          batchRetryLog <- Ref.make(Chunk.empty[(Map[String, Set[PrimaryKey]], Int)])
          batchRetryInterceptor = new BatchRetryInterceptor[Task] {
                                    def onBatchGetRetry(
                                      unprocessedKeys: Map[String, Set[PrimaryKey]],
                                      attempt: Int
                                    ): Task[Unit] =
                                      batchRetryLog.update(_ :+ ((unprocessedKeys, attempt)))
                                    def onBatchWriteRetry(
                                      unprocessedPuts: Map[String, Chunk[Item]],
                                      unprocessedDeletes: Map[String, Chunk[PrimaryKey]],
                                      attempt: Int
                                    ): Task[Unit] = ZIO.unit
                                  }
          interp        <- makeInterp(
                             batchGetResponses = List(
                               DynamoDBQuery.BatchGetItem.Response(unprocessedKeys = unprocessedKeys),
                               DynamoDBQuery.BatchGetItem.Response()
                             ),
                             batchRetryInterceptorParam = Some(batchRetryInterceptor)
                           )
          query =
            DynamoDBQuery
              .batchGetItem(List("a"))(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id)))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber         <- interp.run(query).fork
          _             <- TestClock.adjust(50.millis)
          result        <- fiber.join
          log           <- batchRetryLog.get
        } yield assert(result)(isSubtype[Batch.GetResult.Complete](anything)) &&
          assertTrue(log == Chunk((Map("t" -> Set(PrimaryKey("id" -> "a"))), 0)))
      },
      test("onBatchWriteRetry fires with puts/deletes split from the order-preserving Chunk[Write]") {
        import scala.collection.immutable.{ Map => ScalaMap }
        val putItem          = Item("id" -> "a", "name" -> "alice")
        val deleteKey        = PrimaryKey("id" -> "b")
        val unprocessedItems = ScalaMap(
          "t" -> Chunk[DynamoDBQuery.BatchWriteItem.Write](
            DynamoDBQuery.BatchWriteItem.Put(putItem),
            DynamoDBQuery.BatchWriteItem.Delete(deleteKey)
          )
        )
        for {
          batchRetryLog <- Ref.make(Chunk.empty[(Map[String, Chunk[Item]], Map[String, Chunk[PrimaryKey]], Int)])
          batchRetryInterceptor = new BatchRetryInterceptor[Task] {
                                    def onBatchGetRetry(
                                      unprocessedKeys: Map[String, Set[PrimaryKey]],
                                      attempt: Int
                                    ): Task[Unit] = ZIO.unit
                                    def onBatchWriteRetry(
                                      unprocessedPuts: Map[String, Chunk[Item]],
                                      unprocessedDeletes: Map[String, Chunk[PrimaryKey]],
                                      attempt: Int
                                    ): Task[Unit] =
                                      batchRetryLog.update(_ :+ ((unprocessedPuts, unprocessedDeletes, attempt)))
                                  }
          interp        <- makeInterp(
                             batchWriteResponses = List(
                               DynamoDBQuery.BatchWriteItem.Response(unprocessedItems = Some(unprocessedItems)),
                               DynamoDBQuery.BatchWriteItem.Response(None)
                             ),
                             batchRetryInterceptorParam = Some(batchRetryInterceptor)
                           )
          writes: List[Either[Item, PrimaryKey]] = List(Left(putItem), Right(deleteKey))
          query =
            DynamoDBQuery
              .batchWriteItem(writes) {
                case Left(item) => DynamoDBQuery.PutItem("t", item)
                case Right(key) => DynamoDBQuery.DeleteItem("t", key)
              }
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber         <- interp.run(query).fork
          _             <- TestClock.adjust(50.millis)
          result        <- fiber.join
          log           <- batchRetryLog.get
        } yield assert(result)(isSubtype[Batch.WriteResult.Complete](anything)) &&
          assertTrue(
            log == Chunk((Map("t" -> Chunk(putItem)), Map("t" -> Chunk(deleteKey)), 0))
          )
      }
    ),

    suite("RetryQuota")(
      test("a quota with insufficient budget denies the retry immediately — same outcome shape as curve-exhaustion") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException")),
                      retryQuotaParam = Some(ZioRetryQuota.standard(capacity = 0))
                    )
          query =
            DynamoDBQuery
              .getItem("t", PrimaryKey("id" -> "alice"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(5, FiniteDuration(50, MILLISECONDS), jitter = false))
          result <- interp.run(query).exit
          n      <- calls.get
        } yield assertTrue(result.isFailure && n == 1)
      },
      test("a quota credits back exactly what a successful retry cost — budget recovers for the next call") {
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                        // odd calls fail, even calls succeed — each of the two queries below
                        // fails once then succeeds on its retry.
                        if (n % 2 == 1) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                        else ZIO.succeed(Some(Item("id" -> "alice")))
                      },
                      // exactly one throttling retry's cost — the second query only succeeds if
                      // the first query's successful retry credited its 5 tokens back.
                      retryQuotaParam = Some(ZioRetryQuota.standard(capacity = 5))
                    )
          query =
            DynamoDBQuery
              .getItem("t", PrimaryKey("id" -> "alice"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber1 <- interp.run(query).fork
          _      <- TestClock.adjust(50.millis)
          r1     <- fiber1.join.exit
          fiber2 <- interp.run(query).fork
          _      <- TestClock.adjust(50.millis)
          r2     <- fiber2.join.exit
        } yield assertTrue(r1.isSuccess, r2.isSuccess)
      },
      test("a quota's budget is genuinely shared across concurrent executions, not isolated per call") {
        // Effect always fails, so the only thing bounding total call count is the quota — no
        // assumption about how the two concurrent calls interleave. With capacity == one
        // retry's cost, shared across both, exactly 3 calls can ever happen (2 first attempts
        // + 1 shared retry) regardless of scheduling order; unshared (independent per call)
        // would allow 4 (2 first attempts + 1 retry each).
        for {
          calls  <- Ref.make(0)
          interp <- makeInterp(
                      getItemEffect = calls.updateAndGet(_ + 1) *>
                        ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException")),
                      retryQuotaParam = Some(ZioRetryQuota.standard(capacity = 5))
                    )
          query =
            DynamoDBQuery
              .getItem("t", PrimaryKey("id" -> "alice"))
              .withRetryPolicy(RetryPolicy.ExponentialBackoff(5, FiniteDuration(50, MILLISECONDS), jitter = false))
          fiber1 <- interp.run(query).exit.fork
          fiber2 <- interp.run(query).exit.fork
          _      <- TestClock.adjust(50.millis)
          r1     <- fiber1.join
          r2     <- fiber2.join
          n      <- calls.get
        } yield assertTrue(r1.isFailure, r2.isFailure, n == 3)
      },
      test("a clean (no-retry) success credits exactly 1 token — wired through from a real query") {
        for {
          quota       <- ZIO.succeed(ZioRetryQuota.standard(capacity = 5))
          // Drain the quota to 0 permanently: one throttling retry (cost 5), then a second,
          // non-retryable failure — the spend is never credited back, since only success
          // credits.
          drainCalls  <- Ref.make(0)
          drainInterp <- makeInterp(
                           getItemEffect = drainCalls.updateAndGet(_ + 1).flatMap { n =>
                             if (n == 1) ZIO.fail(new RuntimeException("ProvisionedThroughputExceededException"))
                             else ZIO.fail(new RuntimeException("boom")) // not retryable — permanent
                           },
                           retryQuotaParam = Some(quota)
                         )
          drainPolicy = RetryPolicy.ExponentialBackoff(3, FiniteDuration(50, MILLISECONDS), jitter = false)
          drainQuery = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "x")).withRetryPolicy(drainPolicy)
          drainFiber  <- drainInterp.run(drainQuery).exit.fork
          _           <- TestClock.adjust(50.millis)
          _           <- drainFiber.join // balance is now 0, permanently
          // A fresh call that succeeds on the first attempt (no retry at all) should credit +1.
          // A policy must still be attached — with none at all, `withOptionalRetry` bypasses
          // `withRetry` (and thus crediting) entirely via its `case None => fa` shortcut.
          cleanInterp <- makeInterp(
                           getItemEffect = ZIO.succeed(Some(Item("id" -> "alice"))),
                           retryQuotaParam = Some(quota)
                         )
          cleanQuery = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice")).withRetryPolicy(drainPolicy)
          cleanResult <- cleanInterp.run(cleanQuery)
          // Exactly 1 was credited: a 1-token debit succeeds, but nothing is left for a real
          // (5-token) retry cost afterward.
          smallOk     <- quota.tryConsume(1)
          bigDenied   <- quota.tryConsume(5)
        } yield assertTrue(cleanResult.contains(Item("id" -> "alice")), smallOk, !bigDenied)
      },
      test("credit never pushes the quota's balance above its original capacity") {
        for {
          quota  <- ZIO.succeed(ZioRetryQuota.standard(capacity = 5))
          _      <- quota.credit(1000) // nothing was ever spent; should clamp at capacity
          first  <- quota.tryConsume(5)
          second <- quota.tryConsume(1)
        } yield assertTrue(first, !second)
      }
    ),

    suite("ZioRetryPolicies.fullJitter")(
      test("stops after maxRetries and stays within [0, cap]") {
        for {
          policy  <- ZIO.succeed(
                       ZioRetryPolicies.fullJitter(
                         maxRetries = 3,
                         baseDelay = FiniteDuration(50, MILLISECONDS),
                         maxDelay = FiniteDuration(5, "seconds")
                       )
                     )
          attempt <- policy.newAttempt()
          d0      <- attempt.nextDelay(0)
          d1      <- attempt.nextDelay(1)
          d2      <- attempt.nextDelay(2)
          d3      <- attempt.nextDelay(3)
        } yield assertTrue(
          d3.isEmpty,
          List(d0, d1, d2).forall(_.exists(d => d.toMillis >= 0L && d.toMillis <= 5000L))
        )
      }
    ),

    suite("zipPar")(
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
                          responses = Map("t" -> Chunk(itemA)),
                          unprocessedKeys = unprocessedKeys
                        ),
                        DynamoDBQuery.BatchGetItem.Response(
                          responses = Map("t" -> Chunk(itemB))
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
            hasField("responses", _.response.responses.getOrElse("t", Chunk.empty), equalTo(Chunk(itemA, itemB)))
          )
        )
      }
    )
  )
}
