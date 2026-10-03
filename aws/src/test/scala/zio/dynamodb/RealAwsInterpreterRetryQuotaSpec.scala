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
  BatchGetItemRequest,
  BatchGetItemResponse,
  BatchWriteItemRequest,
  BatchWriteItemResponse,
  CreateTableRequest,
  CreateTableResponse,
  DeleteItemRequest,
  DeleteItemResponse,
  DeleteTableRequest,
  DeleteTableResponse,
  DescribeTableRequest,
  DescribeTableResponse => AwsDescribeTableResponse,
  GetItemRequest,
  GetItemResponse,
  InternalServerErrorException,
  ProvisionedThroughputExceededException,
  PutItemRequest,
  PutItemResponse,
  QueryRequest,
  QueryResponse,
  RequestLimitExceededException,
  ScanRequest,
  ScanResponse,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  UpdateItemRequest,
  UpdateItemResponse
}
import zio.test._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration

/**
 * Full-stack `RetryQuota`: `DummyIOInterpreter` (a real `RealAwsInterpreter`) wired to a stub
 * `AwsDynamoDB[DummyIO]` client throwing real, SDK-generated exceptions, with a real
 * `RetryQuota[DummyIO]` attached — exercising `RealAwsInterpreter.retryCost`'s SDK-exception
 * cost split (5 throttling / 14 transient) end-to-end, at AWS's own real default capacity
 * (500), rather than the tiny artificial capacities (0, 5) the hand-rolled-stub unit specs use
 * to isolate the mechanism. Two volumes: comfortably under capacity (mixed error types, budget
 * recovers via crediting, nothing ever denied) and over capacity (a sustained failure drains
 * the budget until the quota — not the backoff curve — is what stops it).
 */
object RealAwsInterpreterRetryQuotaSpec extends ZIOSpecDefault {

  private def withErrorCode(errorCode: String, statusCode: Int) =
    AwsErrorDetails.builder().errorCode(errorCode).build() -> statusCode

  // Single-threaded, synchronous — DummyIO never runs concurrently, so a plain var is safe
  // (same reasoning FutureRetryPolicies gives for its own per-execution var, just module-wide
  // here since DummyIO has no real concurrency at all).
  private def dummyRetryQuota(capacity: Int): RetryQuota[DummyIO] =
    new RetryQuota[DummyIO] {
      private var balance = capacity

      def tryConsume(cost: Int): DummyIO[Boolean] =
        DummyIO { () =>
          if (balance >= cost) { balance -= cost; true }
          else false
        }

      def credit(amount: Int): DummyIO[Unit] =
        DummyIO { () => balance = math.min(capacity, balance + amount) }
    }

  // -- Stub AwsDynamoDB[DummyIO] client ---------------------------------------
  // getItem calls `nextOutcome()` each time (and increments the returned counter) — the
  // caller controls the exact sequence of failures/successes. Every other method is never
  // called.

  private def getItemClient(
    nextOutcome: () => Either[Throwable, GetItemResponse]
  ): (AwsDynamoDB[DummyIO], () => Int) = {
    val calls  = new AtomicInteger(0)
    val client = new AwsDynamoDB[DummyIO] {
      def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  =
        DummyIO { () =>
          calls.incrementAndGet()
          nextOutcome() match {
            case Left(t)     => throw t
            case Right(resp) => resp
          }
        }
      def putItem(req: PutItemRequest): DummyIO[PutItemResponse]                                  = DummyIO.succeed(???)
      def updateItem(req: UpdateItemRequest): DummyIO[UpdateItemResponse]                         = DummyIO.succeed(???)
      def deleteItem(req: DeleteItemRequest): DummyIO[DeleteItemResponse]                         = DummyIO.succeed(???)
      def batchGetItem(req: BatchGetItemRequest): DummyIO[BatchGetItemResponse]                   = DummyIO.succeed(???)
      def batchWriteItem(req: BatchWriteItemRequest): DummyIO[BatchWriteItemResponse]             = DummyIO.succeed(???)
      def query(req: QueryRequest): DummyIO[QueryResponse]                                        = DummyIO.succeed(???)
      def scan(req: ScanRequest): DummyIO[ScanResponse]                                           = DummyIO.succeed(???)
      def createTable(req: CreateTableRequest): DummyIO[CreateTableResponse]                      = DummyIO.succeed(???)
      def deleteTable(req: DeleteTableRequest): DummyIO[DeleteTableResponse]                      = DummyIO.succeed(???)
      def describeTable(req: DescribeTableRequest): DummyIO[AwsDescribeTableResponse]             = DummyIO.succeed(???)
      def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]       = DummyIO.succeed(???)
      def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse] = DummyIO.succeed(???)
    }
    (client, () => calls.get())
  }

  private val emptyGetItemResponse: GetItemResponse = GetItemResponse.builder().build()

  private def throttling(errorCode: String): Throwable = {
    val (details, status) = withErrorCode(errorCode, 400)
    ProvisionedThroughputExceededException.builder().awsErrorDetails(details).statusCode(status).build()
  }

  private def requestLimitExceeded: Throwable = {
    val (details, status) = withErrorCode("RequestLimitExceeded", 400)
    RequestLimitExceededException.builder().awsErrorDetails(details).statusCode(status).build()
  }

  private def transient5xx(statusCode: Int): Throwable =
    InternalServerErrorException.builder().statusCode(statusCode).build()

  private def runGetItem(
    client: AwsDynamoDB[DummyIO],
    quota: RetryQuota[DummyIO],
    maxRetries: Int
  ): scala.util.Try[Option[Item]] = {
    val interp = new DummyIOInterpreter(client, retryQuota = Some(quota))
    val query  =
      DynamoDBQuery
        .getItem("t", PrimaryKey("id" -> "a"))
        .withRetryPolicy(RetryPolicy.ExponentialBackoff(maxRetries, FiniteDuration(1, "milliseconds"), jitter = false))
    scala.util.Try(interp.run(query).unsafeRun())
  }

  def spec = suite("RealAwsInterpreter — full-stack RetryQuota (real AWS exceptions, AWS's own 500-token default)")(
    test("below threshold: a realistic mix of throttling/transient blips, each costing well under capacity") {
      // 5 calls in a row through one shared quota, each failing once (a different real
      // exception each time — both throttling-tier and transient-tier) then succeeding.
      // Total cost across all 5 retries: 5 + 5 + 5 + 14 + 14 = 43, under the 500-token
      // capacity even before any crediting — and every successful retry credits its own
      // cost straight back, so the budget is effectively untouched afterward.
      val failures         = List(
        throttling("ProvisionedThroughputExceededException"),
        requestLimitExceeded,
        throttling("ThrottlingException"),
        transient5xx(500),
        transient5xx(503)
      )
      val quota            = dummyRetryQuota(capacity = 500)
      val results          = failures.map { err =>
        var failedOnce  = false
        val (client, _) = getItemClient { () =>
          if (!failedOnce) { failedOnce = true; Left(err) }
          else Right(emptyGetItemResponse)
        }
        runGetItem(client, quota, maxRetries = 5)
      }
      // A 6th call, needing its own retry, still succeeds — proving the shared budget
      // wasn't quietly drained by the previous five.
      var sixthFailedOnce  = false
      val (sixthClient, _) = getItemClient { () =>
        if (!sixthFailedOnce) { sixthFailedOnce = true; Left(throttling("ThrottlingException")) }
        else Right(emptyGetItemResponse)
      }
      val sixthResult      = runGetItem(sixthClient, quota, maxRetries = 5)
      assertTrue(results.forall(_.isSuccess), sixthResult.isSuccess)
    },
    test("above threshold: a sustained transient failure drains the budget — the quota, not the curve, stops it") {
      // Every call fails with a real 14-token transient error, forever — maxRetries is set
      // far higher than the budget could ever sustain, so the quota (not curve-exhaustion)
      // is unambiguously what ends the sequence. capacity 500 / cost 14 = 35 affordable
      // retries (35*14=490, 10 left over — not enough for a 36th); the loop runs the initial
      // attempt plus exactly those 35 retries, then the 36th retry is denied outright.
      val (client, calls) = getItemClient(() => Left(transient5xx(500)))
      val quota           = dummyRetryQuota(capacity = 500)
      val result          = runGetItem(client, quota, maxRetries = 1000)
      assertTrue(result.isFailure, calls() == 36)
    }
  )
}
