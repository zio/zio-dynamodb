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

import com.sun.net.httpserver.{ HttpExchange, HttpHandler, HttpServer }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.metrics.CoreMetric
import software.amazon.awssdk.metrics.{ MetricCollection, MetricPublisher }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import zio._
import zio.test._

import java.net.{ InetSocketAddress, ServerSocket, URI }
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/**
 * Direct, empirical evidence that the AWS SDK itself retries by default — independent of
 * zio-dynamodb's own retry machinery entirely (no zio-dynamodb code is exercised here at all).
 * `docs/reference/retries.md` asserts this repeatedly ("the AWS SDK client's own standard
 * retry mode is already on by default"), but nothing in this repo previously demonstrated it.
 *
 * A `MetricPublisher` attached via `ClientOverrideConfiguration` captures the SDK's own
 * self-reported metrics from one logical call, published once regardless of outcome — exact
 * attempt counts, not something inferred from elapsed time. Two independent failure modes are
 * checked (a connection-level failure, nothing listening on the port, and an HTTP-level
 * failure, a real local server that always returns 500) and agree exactly with each other and
 * with two independent metrics within the same call (`CoreMetric.RETRY_COUNT` and the number
 * of per-attempt child `MetricCollection`s): 8 retries, 9 total attempts — matching the AWS
 * SDK's own documented DynamoDB-specific default. See each test's own comment for the exact
 * source wording and why this corrects an earlier misreading that had propagated into this
 * repo's own docs and defaults.
 */
object AwsSdkDefaultRetryBehaviorSpec extends ZIOSpecDefault {

  // Binds to port 0 (OS-assigned free port), then immediately releases it — guaranteed nothing
  // is listening there when the client connects moments later.
  private def unusedLocalPort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  private final class CapturingMetricPublisher extends MetricPublisher {
    val lastCollection: AtomicReference[Option[MetricCollection]] = new AtomicReference(None)
    def publish(metricCollection: MetricCollection): Unit         = lastCollection.set(Some(metricCollection))
    def close(): Unit                                             = ()

    // CoreMetric.RETRY_COUNT's own javadoc: "the number of retries... 0 implies the request
    // worked the first time" — i.e. retries only, excluding the initial attempt.
    def retryCount: Option[Int] =
      lastCollection.get().flatMap(_.metricValues(CoreMetric.RETRY_COUNT).asScala.headOption.map(_.intValue()))

    // One child MetricCollection per attempt (including the initial one) — an independent
    // confirmation of the total attempt count, from a different part of the SDK's own
    // instrumentation than retryCount.
    def attemptCount: Option[Int] =
      lastCollection.get().map(_.children().size())
  }

  private def clientWith(endpoint: URI, publisher: MetricPublisher): DynamoDbAsyncClient =
    DynamoDbAsyncClient
      .builder()
      .endpointOverride(endpoint)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
      .region(Region.US_EAST_1)
      .overrideConfiguration(ClientOverrideConfiguration.builder().addMetricPublisher(publisher).build())
      .build()

  // A real local HTTP server that always responds 500 — an actual HTTP-level failure, as
  // opposed to a connection-level one (nothing listening at all).
  private def alwaysFailingServer(): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext(
      "/",
      new HttpHandler {
        def handle(exchange: HttpExchange): Unit = {
          val body = "Internal Server Error".getBytes
          exchange.sendResponseHeaders(500, body.length.toLong)
          exchange.getResponseBody.write(body)
          exchange.getResponseBody.close()
        }
      }
    )
    server.start()
    server
  }

  private def runAndCapture(client: DynamoDbAsyncClient): Task[Exit[Throwable, _]] =
    ZIO.fromCompletableFuture(client.getItem(GetItemRequest.builder().tableName("doesnt-matter").build())).exit

  def spec = suite("AwsSdkDefaultRetryBehaviorSpec")(
    test("connection-level failure (nothing listening): the SDK retries 8 times — 9 attempts total") {
      val publisher = new CapturingMetricPublisher
      val client    = clientWith(URI.create(s"http://localhost:${unusedLocalPort()}"), publisher)

      for {
        exit <- runAndCapture(client)
        _    <- ZIO.attempt(client.close())
      } yield assertTrue(
        exit.isFailure,
        publisher.retryCount.contains(8),
        publisher.attemptCount.contains(9)
      )
    },
    test("HTTP-level failure (real server, always 500): the SDK retries 8 times — 9 attempts total") {
      val publisher = new CapturingMetricPublisher
      val server    = alwaysFailingServer()
      val client    = clientWith(URI.create(s"http://localhost:${server.getAddress.getPort}"), publisher)

      for {
        exit <- runAndCapture(client)
        _    <- ZIO.attempt(client.close())
        _    <- ZIO.attempt(server.stop(0))
      } yield assertTrue(
        exit.isFailure,
        // AWS's own "Configure retry behavior in the AWS SDK for Java 2.x" guide: "DynamoDB
        // clients use a default maximum retry count of 8 for all retry strategies" — retry
        // *count*, not total attempts: 8 retries, 9 total attempts (confirmed by two
        // independent metrics within the same call, both asserted here and above).
        publisher.retryCount.contains(8),
        publisher.attemptCount.contains(9)
      )
    }
  )
}
