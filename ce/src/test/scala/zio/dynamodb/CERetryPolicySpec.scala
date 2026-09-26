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
    defaultRetryPolicyParam: Option[EffectfulRetryPolicy[IO]] = None
  ): AwsInterpreter[IO] =
    new AwsInterpreter[IO] {
      override protected def defaultRetryPolicy: Option[EffectfulRetryPolicy[IO]] = defaultRetryPolicyParam

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
}
