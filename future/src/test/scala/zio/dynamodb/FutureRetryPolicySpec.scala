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

import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError
import zio.test._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object FutureRetryPolicySpec extends ZIOSpecDefault {

  private def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  // Minimal AwsInterpreter[Future], mirroring zio/RetrySpec.scala's makeInterp stub.
  private def makeInterp(
    getItemEffect: () => Future[Option[Item]],
    defaultRetryPolicyParam: Option[EffectfulRetryPolicy[Future]] = None
  ): AwsInterpreter[Future] =
    new AwsInterpreter[Future] {
      override protected def defaultRetryPolicy: Option[EffectfulRetryPolicy[Future]] = defaultRetryPolicyParam

      private[dynamodb] def pure[A](a: A): Future[A]                                   = Future.successful(a)
      private[dynamodb] def map[A, B](fa: Future[A])(f: A => B): Future[B]             = fa.map(f)
      private[dynamodb] def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
      protected def product[A, B](fa: Future[A], fb: Future[B]): Future[(A, B)]        = fa.zip(fb)
      protected def productPar[A, B](fa: Future[A], fb: Future[B]): Future[(A, B)]     = fa.zip(fb)
      protected def fail[A](e: DynamoDBError): Future[A]                               = Future.failed(e)
      protected def absolve[A](fa: Future[Either[ItemError, A]]): Future[A]            =
        fa.flatMap {
          case Right(a) => Future.successful(a)
          case Left(e)  => Future.failed(e)
        }

      private[dynamodb] def sleep(d: FiniteDuration): Future[Unit] =
        Future.successful(()) // no real delay needed — this suite doesn't assert on timing
      private[dynamodb] def attempt[A](fa: Future[A]): Future[Either[Throwable, A]] =
        fa.transform(t => scala.util.Success(t.toEither))
      private[dynamodb] def raiseError[A](t: Throwable): Future[A]                  = Future.failed(t)

      protected def runGetItem(q: DynamoDBQuery.GetItem): Future[Option[Item]]                                    = getItemEffect()
      protected def runPutItem(q: DynamoDBQuery.PutItem): Future[Option[Item]]                                    = Future.successful(None)
      protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): Future[Option[Item]]                              = Future.successful(None)
      protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): Future[Option[Item]]                              = Future.successful(None)
      protected def runQuery(q: DynamoDBQuery.Query): Future[Page[Item]]                                          =
        Future.successful(Page(Chunk.empty, None, 0, 0))
      protected def runScan(q: DynamoDBQuery.Scan): Future[Page[Item]]                                            =
        Future.successful(Page(Chunk.empty, None, 0, 0))
      protected def runCreateTable(q: DynamoDBQuery.CreateTable): Future[Unit]                                    = Future.unit
      protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): Future[Unit]                                    = Future.unit
      protected def runDescribeTable(q: DynamoDBQuery.DescribeTable): Future[DynamoDBQuery.DescribeTableResponse] =
        Future.successful(DynamoDBQuery.DescribeTableResponse("arn:stub", DynamoDBQuery.TableStatus.Active, 0L, 0L))
      protected def runBatchGetItem(q: DynamoDBQuery.BatchGetItem): Future[DynamoDBQuery.BatchGetItem.Response]   =
        Future.successful(DynamoDBQuery.BatchGetItem.Response())
      protected def runBatchWriteItem(
        q: DynamoDBQuery.BatchWriteItem
      ): Future[DynamoDBQuery.BatchWriteItem.Response]                                                            =
        Future.successful(DynamoDBQuery.BatchWriteItem.Response(None))
      protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): Future[Chunk[Option[Item]]]           =
        Future.successful(Chunk.fill(q.getItems.length)(None))
      protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): Future[Unit]                      = Future.unit
    }

  // A minimal stateless EffectfulRetryPolicy[Future], simpler than FutureRetryPolicies.statefulCustom
  // for the fixed, attempt-only delay functions these two tests need.
  private def statelessRetryPolicy(f: Int => Option[FiniteDuration]): EffectfulRetryPolicy[Future] =
    new EffectfulRetryPolicy[Future] {
      def newAttempt(): Future[EffectfulRetryPolicy.Attempt[Future]] =
        Future.successful(new EffectfulRetryPolicy.Attempt[Future] {
          def nextDelay(attempt: Int): Future[Option[FiniteDuration]] = Future.successful(f(attempt))
        })
    }

  def spec = suite("FutureRetryPolicySpec")(
    test("getItem falls back to defaultRetryPolicy when it has no retryPolicy of its own") {
      val calls  = new AtomicInteger(0)
      val interp = makeInterp(
        getItemEffect = () => {
          val n = calls.incrementAndGet()
          if (n < 2) Future.failed(new RuntimeException("ProvisionedThroughputExceededException"))
          else Future.successful(Some(Item("id" -> "alice")))
        },
        defaultRetryPolicyParam = Some(statelessRetryPolicy(attempt => if (attempt >= 3) None else Some(1.millis)))
      )
      val result = await(interp.run(DynamoDBQuery.getItem("t", PrimaryKey("id" -> "alice"))))
      assertTrue(result.contains(Item("id" -> "alice")) && calls.get() == 2)
    },
    test("a query's own retryPolicy takes precedence over defaultRetryPolicy") {
      val calls  = new AtomicInteger(0)
      val interp = makeInterp(
        getItemEffect = () => {
          calls.incrementAndGet()
          Future.failed(new RuntimeException("ProvisionedThroughputExceededException"))
        },
        defaultRetryPolicyParam = Some(statelessRetryPolicy(_ => Some(1.millis))) // would always retry
      )
      val query  = DynamoDBQuery.getItem("t", PrimaryKey("id" -> "x")).withRetryPolicy(RetryPolicy.NoRetry)
      val result = scala.util.Try(await(interp.run(query)))
      assertTrue(result.isFailure && calls.get() == 1) // NoRetry wins — defaultRetryPolicy never consulted
    },
    test("FutureRetryPolicies.statefulCustom scopes state per newAttempt() call") {
      val policy = FutureRetryPolicies.statefulCustom(initial = 0) { (count, _) =>
        (count + 1, Some(FiniteDuration((count + 1).toLong, "milliseconds")))
      }
      val first  = await(policy.newAttempt())
      val second = await(policy.newAttempt())
      assertTrue(
        await(first.nextDelay(0)).contains(1.millis) &&
          await(first.nextDelay(0)).contains(2.millis) &&
          await(second.nextDelay(0)).contains(1.millis) // fresh state — not 3.millis
      )
    },
    test("FutureRetryPolicies.awsRecommended stops after maxRetries and stays within [base, cap] otherwise") {
      val policy  = FutureRetryPolicies.awsRecommended(maxRetries = 3, baseDelay = 50.millis, maxDelay = 5.seconds)
      val attempt = await(policy.newAttempt())
      val delays  = (0 until 3).map(n => await(attempt.nextDelay(n)).get.toMillis)
      val last    = await(attempt.nextDelay(3))
      assertTrue(delays.forall(d => d >= 50L && d <= 5000L) && last.isEmpty)
    }
  )
}
