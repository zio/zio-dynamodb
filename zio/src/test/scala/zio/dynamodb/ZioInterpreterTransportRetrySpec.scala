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

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.test._

import java.net.{ ServerSocket, URI }
import java.time.{ Duration => JDuration }
import scala.concurrent.duration.FiniteDuration

/**
 * Proves a real transport-level failure (a genuine `SdkClientException`, not a mock) is
 * retried by zio-dynamodb's own effect-level retry loop, not just classified correctly in
 * isolation. `RealAwsInterpreterRetrySpec` (`aws` module) only unit-tests `isRetryable`'s
 * predicate directly; this exercises the real `DynamoDbAsyncClient`,
 * `ZioInterpreter.fromAsyncClient`'s `defaultRetryPolicy`, and `RetryInterceptor` together,
 * against the exact exception class that classification covers.
 *
 * The target port is a `ServerSocket` bound and held open — never accepting, never released
 * early — for the whole test, rather than the classic "bind, read the port, close, hope
 * nothing else grabs it before we connect" pattern, which has a real (if narrow) TOCTOU race.
 * A connection to a listening-but-never-accepting socket completes the TCP handshake (the OS
 * queues it) but gets no response, so a short `apiCallAttemptTimeout` is what actually
 * produces the transport-level failure (`ApiCallAttemptTimeoutException`, a `SdkClientException`
 * subtype) this test exercises. Both the socket and the client are acquired via
 * `ZIO.fromAutoCloseable` inside `ZIO.scoped` so cleanup runs even if the test is interrupted
 * (e.g. by `TestAspect.timeout`), not only on normal completion.
 *
 * The SDK client's own retry strategy is capped at 1 attempt so only zio-dynamodb's own retry
 * loop is under test here — matching `docs/reference/retries.md`'s recommended split of the two
 * layers. `maxRetries`/`baseDelay` are kept small since the backoff sleeps are real wall-clock
 * time here (`TestAspect.withLiveClock`) — the retry loop's `ZIO.sleep` would otherwise wait
 * forever on zio-test's default virtual `TestClock`, which nothing in this test advances.
 */
object ZioInterpreterTransportRetrySpec extends ZIOSpecDefault {

  private def clientWith(endpoint: URI): DynamoDbAsyncClient =
    DynamoDbAsyncClient
      .builder()
      .endpointOverride(endpoint)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
      .region(Region.US_EAST_1)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .apiCallAttemptTimeout(JDuration.ofMillis(300))
          .retryStrategy(AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(1).build())
          .build()
      )
      .build()

  def spec = suite("ZioInterpreterTransportRetrySpec")(
    test("a real transport-level failure is retried by zio-dynamodb's own retry loop") {
      ZIO.scoped {
        for {
          socket     <- ZIO.fromAutoCloseable(ZIO.attempt(new ServerSocket(0)))
          retryCount <- Ref.make(0)
          interceptor = new RetryInterceptor[Task] {
                          def onRetry(meta: DynamoDBRetryMetadata, error: Throwable, attempt: Int): Task[Unit] =
                            retryCount.update(_ + 1)
                        }
          client     <-
            ZIO.fromAutoCloseable(ZIO.attempt(clientWith(URI.create(s"http://localhost:${socket.getLocalPort}"))))
          interp = ZioInterpreter.fromAsyncClient(
                     client,
                     interceptors = InterceptorConfig(retry = Some(interceptor)),
                     defaultRetryPolicy = Some(
                       ZioRetryPolicies.fullJitter(
                         maxRetries = 2,
                         baseDelay = FiniteDuration(10, "milliseconds"),
                         maxDelay = FiniteDuration(50, "milliseconds")
                       )
                     )
                   )
          exit       <- interp.run(DynamoDBQuery.getItem("doesnt-matter", PrimaryKey("id" -> "x"))).exit
          count      <- retryCount.get
        } yield assertTrue(exit.isFailure, count == 2)
      }
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(zio.Duration.fromSeconds(20))
  )
}
