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

import cats.effect.{ IO, Ref }
import munit.CatsEffectSuite
import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError

import scala.concurrent.duration.FiniteDuration

// Unit-level (no Testcontainers/Docker needed) — mirrors zio/RetrySpec.scala's coverage of
// EffectfulRetryPolicy/defaultRetryPolicy, adapted to this module's IO + munit conventions.
class CERetryPolicySpec extends CatsEffectSuite {

  // Minimal AwsInterpreter[IO], mirroring zio/RetrySpec.scala's makeInterp stub.
  private def makeInterp(
    getItemEffect: IO[Option[Item]],
    defaultRetryPolicyParam: Option[EffectfulRetryPolicy[IO]] = None,
    retryQuotaParam: Option[RetryQuota[IO]] = None
  ): AwsInterpreter[IO] =
    new AwsInterpreter[IO] {
      override protected def defaultRetryPolicy: Option[EffectfulRetryPolicy[IO]] = defaultRetryPolicyParam
      override protected def retryQuota: Option[RetryQuota[IO]]                   = retryQuotaParam

      private[dynamodb] def pure[A](a: A): IO[A]                           = IO.pure(a)
      private[dynamodb] def map[A, B](fa: IO[A])(f: A => B): IO[B]         = fa.map(f)
      private[dynamodb] def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
      protected def product[A, B](fa: IO[A], fb: IO[B]): IO[(A, B)]        = fa.flatMap(a => fb.map(b => (a, b)))
      protected def productPar[A, B](fa: IO[A], fb: IO[B]): IO[(A, B)]     = IO.both(fa, fb)
      protected def fail[A](e: DynamoDBError): IO[A]                       = IO.raiseError(e)
      protected def absolve[A](fa: IO[Either[ItemError, A]]): IO[A]        =
        fa.flatMap {
          case Right(a) => IO.pure(a)
          case Left(e)  => IO.raiseError(e)
        }

      private[dynamodb] def sleep(d: FiniteDuration): IO[Unit]              = IO.sleep(d)
      private[dynamodb] def attempt[A](fa: IO[A]): IO[Either[Throwable, A]] = fa.attempt
      private[dynamodb] def raiseError[A](t: Throwable): IO[A]              = IO.raiseError(t)

      protected def runGetItem(q: DynamoDBQuery.GetItem): IO[Option[Item]]                                        = getItemEffect
      protected def runPutItem(q: DynamoDBQuery.PutItem): IO[Option[Item]]                                        = IO.pure(None)
      protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): IO[Option[Item]]                                  = IO.pure(None)
      protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): IO[Option[Item]]                                  = IO.pure(None)
      protected def runQuery(q: DynamoDBQuery.Query): IO[Page[Item]]                                              =
        IO.pure(Page(Chunk.empty, None, 0, 0))
      protected def runScan(q: DynamoDBQuery.Scan): IO[Page[Item]]                                                =
        IO.pure(Page(Chunk.empty, None, 0, 0))
      protected def runCreateTable(q: DynamoDBQuery.CreateTable): IO[Unit]                                        = IO.unit
      protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): IO[Unit]                                        = IO.unit
      protected def runDescribeTable(q: DynamoDBQuery.DescribeTable): IO[DynamoDBQuery.DescribeTableResponse]     =
        IO.pure(DynamoDBQuery.DescribeTableResponse("arn:stub", DynamoDBQuery.TableStatus.Active, 0L, 0L))
      protected def runBatchGetItem(q: DynamoDBQuery.BatchGetItem): IO[DynamoDBQuery.BatchGetItem.Response]       =
        IO.pure(DynamoDBQuery.BatchGetItem.Response())
      protected def runBatchWriteItem(q: DynamoDBQuery.BatchWriteItem): IO[DynamoDBQuery.BatchWriteItem.Response] =
        IO.pure(DynamoDBQuery.BatchWriteItem.Response(None))
      protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): IO[Chunk[Option[Item]]]               =
        IO.pure(Chunk.fill(q.getItems.length)(None))
      protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): IO[Unit]                          = IO.unit
    }

  test("CatsRetryPolicies.statefulCustom scopes state per newAttempt() call") {
    val policy = CatsRetryPolicies.statefulCustom(initial = 0) { (count, _) =>
      val next = count + 1
      (next, Some(FiniteDuration(next.toLong, "milliseconds")))
    }
    for {
      first        <- policy.newAttempt()
      second       <- policy.newAttempt()
      firstResult1 <- first.nextDelay(0)
      firstResult2 <- first.nextDelay(0)
      secondResult <- second.nextDelay(0) // fresh state — not 3.millis
    } yield {
      assertEquals(firstResult1, Some(FiniteDuration(1, "milliseconds")))
      assertEquals(firstResult2, Some(FiniteDuration(2, "milliseconds")))
      assertEquals(secondResult, Some(FiniteDuration(1, "milliseconds")))
    }
  }

  test("CatsRetryPolicies.fullJitter stops after maxRetries and stays within [0, cap]") {
    val policy = CatsRetryPolicies.fullJitter(
      maxRetries = 3,
      baseDelay = FiniteDuration(50, "milliseconds"),
      maxDelay = FiniteDuration(5, "seconds")
    )
    for {
      attempt <- policy.newAttempt()
      d0      <- attempt.nextDelay(0)
      d1      <- attempt.nextDelay(1)
      d2      <- attempt.nextDelay(2)
      d3      <- attempt.nextDelay(3)
    } yield {
      assertEquals(d3, None)
      assert(List(d0, d1, d2).forall(_.exists(d => d.toMillis >= 0L && d.toMillis <= 5000L)))
    }
  }

  test("getItem falls back to defaultRetryPolicy when it has no retryPolicy of its own") {
    val decorrelatedJitter = CatsRetryPolicies.statefulCustom(initial = 100L) { (previousDelay, attempt) =>
      if (attempt >= 3) (previousDelay, None)
      else (previousDelay, Some(FiniteDuration(1, "milliseconds"))) // short, deterministic delay for the test
    }
    for {
      calls  <- Ref.of[IO, Int](0)
      interp = makeInterp(
                 getItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                   if (n < 2) IO.raiseError(new RuntimeException("ProvisionedThroughputExceededException"))
                   else IO.pure(Some(Item("id" -> "alice")))
                 },
                 defaultRetryPolicyParam = Some(decorrelatedJitter)
               )
      result <- interp.run(DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice")))
      n      <- calls.get
    } yield {
      assertEquals(result, Some(Item("id" -> "alice")))
      assertEquals(n, 2)
    }
  }

  test("a query's own retryPolicy takes precedence over defaultRetryPolicy") {
    val alwaysRetries =
      CatsRetryPolicies.statefulCustom(initial = ())((_, _) => ((), Some(FiniteDuration(1, "milliseconds"))))
    for {
      calls  <- Ref.of[IO, Int](0)
      interp = makeInterp(
                 getItemEffect = calls.updateAndGet(_ + 1) *>
                   IO.raiseError(new RuntimeException("ProvisionedThroughputExceededException")),
                 defaultRetryPolicyParam = Some(alwaysRetries)
               )
      query = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "x")).withRetryPolicy(RetryPolicy.NoRetry)
      result <- interp.run(query).attempt
      n      <- calls.get
    } yield {
      assert(result.isLeft)
      assertEquals(n, 1) // NoRetry wins — defaultRetryPolicy never consulted
    }
  }

  test("a quota with insufficient budget denies the retry immediately — same outcome shape as curve-exhaustion") {
    for {
      calls  <- Ref.of[IO, Int](0)
      interp = makeInterp(
                 getItemEffect = calls.updateAndGet(_ + 1) *>
                   IO.raiseError(new RuntimeException("ProvisionedThroughputExceededException")),
                 retryQuotaParam = Some(CERetryQuota.standard(capacity = 0))
               )
      query =
        DynamoDBQuery
          .getItem("t", PrimaryKey("id" -> "alice"))
          .withRetryPolicy(RetryPolicy.ExponentialBackoff(5, FiniteDuration(1, "milliseconds"), jitter = false))
      result <- interp.run(query).attempt
      n      <- calls.get
    } yield {
      assert(result.isLeft)
      assertEquals(n, 1)
    }
  }

  test("a quota credits back exactly what a successful retry cost — budget recovers for the next call") {
    for {
      calls <- Ref.of[IO, Int](0)
      interp = makeInterp(
                 getItemEffect = calls.updateAndGet(_ + 1).flatMap { n =>
                   // odd calls fail, even calls succeed — each of the two queries below fails
                   // once then succeeds on its retry.
                   if (n % 2 == 1) IO.raiseError(new RuntimeException("ProvisionedThroughputExceededException"))
                   else IO.pure(Some(Item("id" -> "alice")))
                 },
                 // exactly one throttling retry's cost — the second query only succeeds if the
                 // first query's successful retry credited its 5 tokens back.
                 retryQuotaParam = Some(CERetryQuota.standard(capacity = 5))
               )
      query =
        DynamoDBQuery
          .getItem("t", PrimaryKey("id" -> "alice"))
          .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, FiniteDuration(1, "milliseconds"), jitter = false))
      r1    <- interp.run(query).attempt
      r2    <- interp.run(query).attempt
    } yield {
      assert(r1.isRight)
      assert(r2.isRight)
    }
  }

  test("a quota's budget is genuinely shared across concurrent executions, not isolated per call") {
    // Effect always fails, so the only thing bounding total call count is the quota — no
    // assumption about how the two concurrent calls interleave. With capacity == one retry's
    // cost, shared across both, exactly 3 calls can ever happen (2 first attempts + 1 shared
    // retry) regardless of scheduling order; unshared (independent per call) would allow 4
    // (2 first attempts + 1 retry each).
    for {
      calls   <- Ref.of[IO, Int](0)
      interp = makeInterp(
                 getItemEffect = calls.updateAndGet(_ + 1) *>
                   IO.raiseError(new RuntimeException("ProvisionedThroughputExceededException")),
                 retryQuotaParam = Some(CERetryQuota.standard(capacity = 5))
               )
      query =
        DynamoDBQuery
          .getItem("t", PrimaryKey("id" -> "alice"))
          .withRetryPolicy(RetryPolicy.ExponentialBackoff(5, FiniteDuration(1, "milliseconds"), jitter = false))
      results <- (interp.run(query).attempt, interp.run(query).attempt).parTupled
      n       <- calls.get
    } yield assert(results._1.isLeft && results._2.isLeft && n == 3)
  }

  test("a clean (no-retry) success credits exactly 1 token — wired through from a real query") {
    val quota = CERetryQuota.standard(capacity = 5)
    for {
      // Drain the quota to 0 permanently: one throttling retry (cost 5), then a second,
      // non-retryable failure — the spend is never credited back, since only success credits.
      drainCalls  <- Ref.of[IO, Int](0)
      drainInterp = makeInterp(
                      getItemEffect = drainCalls.updateAndGet(_ + 1).flatMap { n =>
                        if (n == 1) IO.raiseError(new RuntimeException("ProvisionedThroughputExceededException"))
                        else IO.raiseError(new RuntimeException("boom")) // not retryable — permanent
                      },
                      retryQuotaParam = Some(quota)
                    )
      drainPolicy = RetryPolicy.ExponentialBackoff(3, FiniteDuration(1, "milliseconds"), jitter = false)
      drainQuery = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "x")).withRetryPolicy(drainPolicy)
      _           <- drainInterp.run(drainQuery).attempt // balance is now 0, permanently
      // A fresh call that succeeds on the first attempt (no retry at all) should credit +1. A
      // policy must still be attached — with none at all, `withOptionalRetry` bypasses
      // `withRetry` (and thus crediting) entirely via its `case None => fa` shortcut.
      cleanInterp = makeInterp(getItemEffect = IO.pure(Some(Item("id" -> "alice"))), retryQuotaParam = Some(quota))
      cleanQuery = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice")).withRetryPolicy(drainPolicy)
      cleanResult <- cleanInterp.run(cleanQuery)
      // Exactly 1 was credited: a 1-token debit succeeds, but nothing is left for a real
      // (5-token) retry cost afterward.
      smallOk     <- quota.tryConsume(1)
      bigDenied   <- quota.tryConsume(5)
    } yield {
      assertEquals(cleanResult, Some(Item("id" -> "alice")))
      assert(smallOk)
      assert(!bigDenied)
    }
  }

  test("credit never pushes the quota's balance above its original capacity") {
    val quota = CERetryQuota.standard(capacity = 5)
    for {
      _      <- quota.credit(1000) // nothing was ever spent; should clamp at capacity
      first  <- quota.tryConsume(5)
      second <- quota.tryConsume(1)
    } yield {
      assert(first)
      assert(!second)
    }
  }
}
