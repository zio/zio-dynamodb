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

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.services.dynamodb.model.{
  InternalServerErrorException,
  ProvisionedThroughputExceededException,
  RequestLimitExceededException,
  ResourceNotFoundException
}
import zio.dynamodb.DynamoDBError.ItemError
import zio.test._

import scala.concurrent.duration.FiniteDuration

/**
 * `RealAwsInterpreter.isRetryable` classifies retryability from the AWS SDK's own exception
 * types rather than [[RetryPolicy.isRetryable]]'s message-substring check. These construct the
 * real generated exception types the way an unmarshalled DynamoDB response actually would
 * (status code + `awsErrorDetails().errorCode()`, not just a message string) to prove the
 * classification matches DynamoDB's real wire shape.
 */
object RealAwsInterpreterRetrySpec extends ZIOSpecDefault {

  // A RealAwsInterpreter[DummyIO] whose client is never called — only `isRetryable` is
  // exercised — with an accessor exposing that otherwise-protected member for the tests below.
  private final class TestInterp extends RealAwsInterpreter[DummyIO](null) {
    private[dynamodb] def pure[A](a: A): DummyIO[A]                                     = DummyIO.succeed(a)
    private[dynamodb] def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              =
      DummyIO(() => f(fa.unsafeRun()))
    private[dynamodb] def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] =
      DummyIO(() => f(fa.unsafeRun()).unsafeRun())
    protected def product[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]        =
      DummyIO(() => (fa.unsafeRun(), fb.unsafeRun()))
    protected def productPar[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]     =
      DummyIO(() => (fa.unsafeRun(), fb.unsafeRun()))
    protected def fail[A](e: DynamoDBError): DummyIO[A]                                 =
      DummyIO(() => throw e)
    protected def absolve[A](fa: DummyIO[Either[ItemError, A]]): DummyIO[A]             =
      DummyIO(() =>
        fa.unsafeRun() match {
          case Right(a) => a
          case Left(e)  => throw e
        }
      )
    private[dynamodb] def sleep(d: FiniteDuration): DummyIO[Unit]                       = DummyIO.succeed(())
    private[dynamodb] def attempt[A](fa: DummyIO[A]): DummyIO[Either[Throwable, A]]     =
      DummyIO(() => scala.util.Try(fa.unsafeRun()).toEither)
    private[dynamodb] def raiseError[A](t: Throwable): DummyIO[A]                       = DummyIO(() => throw t)

    def exposedIsRetryable: Throwable => Boolean = isRetryable
  }

  private val retryable: Throwable => Boolean = new TestInterp().exposedIsRetryable

  private def withErrorCode(errorCode: String, statusCode: Int) =
    AwsErrorDetails.builder().errorCode(errorCode).build() -> statusCode

  def spec = suite("RealAwsInterpreter.isRetryable")(
    suite("throttling — recognized via the SDK's error-code check, matching DynamoDB's real 400 responses")(
      test("ProvisionedThroughputExceededException is retryable") {
        val (details, status) = withErrorCode("ProvisionedThroughputExceededException", 400)
        val e                 =
          ProvisionedThroughputExceededException.builder().awsErrorDetails(details).statusCode(status).build()
        assertTrue(retryable(e))
      },
      test("RequestLimitExceededException is retryable") {
        val (details, status) = withErrorCode("RequestLimitExceeded", 400)
        val e                 = RequestLimitExceededException.builder().awsErrorDetails(details).statusCode(status).build()
        assertTrue(retryable(e))
      },
      test("a generic ThrottlingException error code is retryable regardless of exception subtype") {
        val (details, status) = withErrorCode("ThrottlingException", 400)
        val e                 = InternalServerErrorException.builder().awsErrorDetails(details).statusCode(status).build()
        assertTrue(retryable(e))
      }
    ),
    suite("5xx — transient server-side failures")(
      test("500 Internal Server Error is retryable") {
        assertTrue(retryable(InternalServerErrorException.builder().statusCode(500).build()))
      },
      test("503 Service Unavailable is retryable") {
        assertTrue(retryable(InternalServerErrorException.builder().statusCode(503).build()))
      }
    ),
    suite("not retryable")(
      test("ResourceNotFoundException (a real, permanent 400) is not retryable") {
        val (details, status) = withErrorCode("ResourceNotFoundException", 400)
        val e                 = ResourceNotFoundException.builder().awsErrorDetails(details).statusCode(status).build()
        assertTrue(!retryable(e))
      }
    ),
    suite("non-AwsServiceException — falls back to RetryPolicy.isRetryable")(
      test("a plain throwable whose message matches the substring predicate is still retryable") {
        assertTrue(retryable(new RuntimeException("ThrottlingException")))
      },
      test("a plain throwable with no matching substring is not retryable") {
        assertTrue(!retryable(new RuntimeException("boom")))
      }
    )
  )
}
