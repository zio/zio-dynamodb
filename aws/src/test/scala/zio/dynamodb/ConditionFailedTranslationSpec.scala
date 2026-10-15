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
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AwsAttributeValue,
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
  GetItemRequest,
  GetItemResponse,
  ProvisionedThroughputExceededException,
  PutItemRequest,
  PutItemResponse,
  QueryRequest,
  QueryResponse,
  ScanRequest,
  ScanResponse,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  UpdateItemRequest,
  UpdateItemResponse
}
import zio.dynamodb.DynamoDBError.ItemError.ConditionFailed
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion._
import zio.test._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object ConditionFailedTranslationSpec extends ZIOSpecDefault {

  // put / update / delete throw `failWith`; every other method is never called.
  private def writeClient(failWith: Throwable): (AwsDynamoDB[DummyIO], () => Int) = {
    val calls               = new AtomicInteger(0)
    def fail[A]: DummyIO[A] = DummyIO { () => calls.incrementAndGet(); throw failWith }
    val client              = new AwsDynamoDB[DummyIO] {
      def putItem(req: PutItemRequest): DummyIO[PutItemResponse]                                  = fail
      def updateItem(req: UpdateItemRequest): DummyIO[UpdateItemResponse]                         = fail
      def deleteItem(req: DeleteItemRequest): DummyIO[DeleteItemResponse]                         = fail
      def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  = DummyIO.succeed(???)
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

  private def run[A](client: AwsDynamoDB[DummyIO], q: DynamoDBQuery[_, A]): Either[Throwable, A] =
    scala.util.Try(new DummyIOInterpreter(client).run(q).unsafeRun()).toEither

  private val item = Item("id" -> "a", "v" -> 1)

  private val conditionalPut    = DynamoDBQuery.putItem("t", item).where($("v") === 0)
  private val conditionalUpdate =
    DynamoDBQuery.updateItem("t", PrimaryKey("id" -> "a"))($("v").set(2)).where($("v") === 0)
  private val conditionalDelete = DynamoDBQuery.deleteItem("t", PrimaryKey("id" -> "a")).where($("v") === 0)

  private val ccfeWithItem: ConditionalCheckFailedException =
    ConditionalCheckFailedException
      .builder()
      .message("The conditional request failed")
      .item(Map("id" -> AwsAttributeValue.fromS("a"), "v" -> AwsAttributeValue.fromN("7")).asJava)
      .build()
      .asInstanceOf[ConditionalCheckFailedException]

  private val throttled: ProvisionedThroughputExceededException =
    ProvisionedThroughputExceededException
      .builder()
      .awsErrorDetails(AwsErrorDetails.builder().errorCode("ProvisionedThroughputExceededException").build())
      .statusCode(400)
      .build()
      .asInstanceOf[ProvisionedThroughputExceededException]

  def spec = suite("ConditionalCheckFailedException → ItemError.ConditionFailed (single-item writes)")(
    test("conditional put: translated, with the existing item decoded") {
      val (client, _) = writeClient(ccfeWithItem)
      assert(run(client, conditionalPut))(
        isLeft(
          isSubtype[ConditionFailed](
            hasField[ConditionFailed, Option[Item]]("item", _.item, isSome(equalTo(Item("id" -> "a", "v" -> 7))))
          )
        )
      )
    },
    test("an SDK exception with no message still gives ConditionFailed a message") {
      val noMessage   = ConditionalCheckFailedException.builder().build().asInstanceOf[ConditionalCheckFailedException]
      val (client, _) = writeClient(noMessage)
      assert(run(client, conditionalPut))(
        isLeft(isSubtype[ConditionFailed](hasField[ConditionFailed, String]("message", _.message, isNonEmptyString)))
      )
    },
    test("the SDK exception is kept as getCause") {
      val (client, _) = writeClient(ccfeWithItem)
      assert(run(client, conditionalPut).swap.map(e => Option(e.getCause)))(
        isRight(isSome(isSubtype[ConditionalCheckFailedException](equalTo(ccfeWithItem))))
      )
    },
    test("conditional update and delete: translated too") {
      val (client, _) = writeClient(ccfeWithItem)
      assert(run(client, conditionalUpdate))(isLeft(isSubtype[ConditionFailed](anything))) &&
      assert(run(client, conditionalDelete))(isLeft(isSubtype[ConditionFailed](anything)))
    },
    test("a failed condition is not retried") {
      val (client, calls) = writeClient(ccfeWithItem)
      val q               = conditionalPut.withRetryPolicy(
        RetryPolicy.ExponentialBackoff(5, FiniteDuration(1, "milliseconds"), jitter = false)
      )
      assert(run(client, q))(isLeft(isSubtype[ConditionFailed](anything))) && assertTrue(calls() == 1)
    },
    test("throttling on a conditional write passes through untouched (same exception, not ConditionFailed)") {
      val (client, _) = writeClient(throttled)
      assert(run(client, conditionalPut))(isLeft(isSubtype[ProvisionedThroughputExceededException](equalTo(throttled))))
    },
    test("a client-side failure on a conditional write passes through untouched") {
      val e           = SdkClientException.create("connection reset")
      val (client, _) = writeClient(e)
      assert(run(client, conditionalPut))(isLeft(isSubtype[SdkClientException](equalTo(e))))
    },
    test("an unconditional write is not wrapped (it can't fail a condition)") {
      val (client, _) = writeClient(throttled)
      assert(run(client, DynamoDBQuery.putItem("t", item)))(isLeft(equalTo(throttled: Throwable)))
    }
  )
}
