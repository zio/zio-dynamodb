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
  ConditionalCheckFailedException,
  CreateTableRequest,
  CreateTableResponse,
  DeleteItemRequest,
  DeleteItemResponse,
  DeleteTableRequest,
  DeleteTableResponse,
  DescribeTableRequest,
  DescribeTableResponse => AwsDescribeTableResponse,
  DynamoDbException,
  GetItemRequest,
  GetItemResponse,
  InternalServerErrorException,
  ProvisionedThroughputExceededException,
  PutItemRequest,
  PutItemResponse,
  QueryRequest,
  QueryResponse,
  RequestLimitExceededException,
  ResourceNotFoundException,
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
 * Full-stack retry loop: `DummyIOInterpreter` (a real `RealAwsInterpreter`) wired to a stub
 * `AwsDynamoDB[DummyIO]` client that throws real, SDK-generated exception instances — shaped
 * the way an unmarshalled DynamoDB response actually would be (status code +
 * `awsErrorDetails().errorCode()`), not synthetic `RuntimeException` messages. Where
 * `RealAwsInterpreterRetrySpec` tests `isRetryable`/`retryCost` classification in isolation
 * (`client = null`, never called), this drives the same classifiers through the real
 * `withRetry` loop, across a range of exceptions spanning both tiers AWS itself distinguishes
 * (throttling vs. other transient) and the 400-range exceptions that must NOT retry.
 */
object RealAwsInterpreterRetryLoopSpec extends ZIOSpecDefault {

  private def withErrorCode(errorCode: String, statusCode: Int) =
    AwsErrorDetails.builder().errorCode(errorCode).build() -> statusCode

  // -- Stub AwsDynamoDB[DummyIO] client ---------------------------------------
  // getItem throws `failWith` on the first `failures` calls, then returns `response`.
  // Every other method is never called (throws if it somehow were).

  private def getItemClient(
    failWith: Throwable,
    failures: Int,
    response: GetItemResponse
  ): (AwsDynamoDB[DummyIO], () => Int) = {
    val calls  = new AtomicInteger(0)
    val client = new AwsDynamoDB[DummyIO] {
      def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  =
        DummyIO { () =>
          val n = calls.getAndIncrement()
          if (n < failures) throw failWith else response
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

  private def runGetItem(client: AwsDynamoDB[DummyIO], maxRetries: Int): scala.util.Try[Option[Item]] = {
    val interp = new DummyIOInterpreter(client)
    val query  =
      DynamoDBQuery
        .getItem("t", PrimaryKey("id" -> "a"))
        .withRetryPolicy(RetryPolicy.ExponentialBackoff(maxRetries, FiniteDuration(1, "milliseconds"), jitter = false))
    scala.util.Try(interp.run(query).unsafeRun())
  }

  def spec = suite("RealAwsInterpreter — full-stack retry loop (DummyIOInterpreter + real AWS exceptions)")(
    suite("retryable — real exception, retries then succeeds")(
      test("ProvisionedThroughputExceededException (throttling, 400)") {
        val (details, status) = withErrorCode("ProvisionedThroughputExceededException", 400)
        val e                 =
          ProvisionedThroughputExceededException.builder().awsErrorDetails(details).statusCode(status).build()
        val (client, calls)   = getItemClient(e, failures = 2, emptyGetItemResponse)
        val result            = runGetItem(client, maxRetries = 5)
        assertTrue(result.isSuccess, calls() == 3)
      },
      test("RequestLimitExceededException (throttling, 400)") {
        val (details, status) = withErrorCode("RequestLimitExceeded", 400)
        val e                 = RequestLimitExceededException.builder().awsErrorDetails(details).statusCode(status).build()
        val (client, calls)   = getItemClient(e, failures = 2, emptyGetItemResponse)
        val result            = runGetItem(client, maxRetries = 5)
        assertTrue(result.isSuccess, calls() == 3)
      },
      test("a generic ThrottlingException error code, regardless of exception subtype") {
        val (details, status) = withErrorCode("ThrottlingException", 400)
        val e                 = DynamoDbException.builder().awsErrorDetails(details).statusCode(status).build()
        val (client, calls)   = getItemClient(e, failures = 2, emptyGetItemResponse)
        val result            = runGetItem(client, maxRetries = 5)
        assertTrue(result.isSuccess, calls() == 3)
      },
      test("500 Internal Server Error (transient, 5xx)") {
        val e               = InternalServerErrorException.builder().statusCode(500).build()
        val (client, calls) = getItemClient(e, failures = 2, emptyGetItemResponse)
        val result          = runGetItem(client, maxRetries = 5)
        assertTrue(result.isSuccess, calls() == 3)
      },
      test("503 Service Unavailable (transient, 5xx)") {
        val e               = InternalServerErrorException.builder().statusCode(503).build()
        val (client, calls) = getItemClient(e, failures = 2, emptyGetItemResponse)
        val result          = runGetItem(client, maxRetries = 5)
        assertTrue(result.isSuccess, calls() == 3)
      }
    ),
    suite("not retryable (400 range, not a throttling/transient code) — fails on the first attempt")(
      test("ResourceNotFoundException — permanent, not throttling") {
        val (details, status) = withErrorCode("ResourceNotFoundException", 400)
        val e                 = ResourceNotFoundException.builder().awsErrorDetails(details).statusCode(status).build()
        val (client, calls)   = getItemClient(e, failures = Int.MaxValue, emptyGetItemResponse)
        val result            = runGetItem(client, maxRetries = 5)
        assertTrue(result.isFailure, calls() == 1)
      },
      test("ConditionalCheckFailedException — a conditional write rejection, not throttling") {
        val (details, status) = withErrorCode("ConditionalCheckFailedException", 400)
        val e                 = ConditionalCheckFailedException.builder().awsErrorDetails(details).statusCode(status).build()
        val (client, calls)   = getItemClient(e, failures = Int.MaxValue, emptyGetItemResponse)
        val result            = runGetItem(client, maxRetries = 5)
        assertTrue(result.isFailure, calls() == 1)
      },
      test("a generic ValidationException error code — a malformed request, not throttling") {
        val (details, status) = withErrorCode("ValidationException", 400)
        val e                 = DynamoDbException.builder().awsErrorDetails(details).statusCode(status).build()
        val (client, calls)   = getItemClient(e, failures = Int.MaxValue, emptyGetItemResponse)
        val result            = runGetItem(client, maxRetries = 5)
        assertTrue(result.isFailure, calls() == 1)
      }
    )
  )
}
