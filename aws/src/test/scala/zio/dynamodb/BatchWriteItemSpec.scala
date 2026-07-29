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

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue => AwsAttrValue,
  BatchGetItemRequest,
  BatchGetItemResponse,
  BatchWriteItemRequest,
  BatchWriteItemResponse,
  CreateTableRequest,
  CreateTableResponse,
  DeleteItemRequest,
  DeleteItemResponse,
  DeleteRequest,
  DeleteTableRequest,
  DeleteTableResponse,
  DescribeTableRequest,
  DescribeTableResponse => AwsDescribeTableResponse,
  GetItemRequest,
  GetItemResponse,
  PutItemRequest,
  PutItemResponse,
  PutRequest,
  QueryRequest,
  QueryResponse,
  ScanRequest,
  ScanResponse,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  UpdateItemRequest,
  UpdateItemResponse,
  WriteRequest
}
import zio.test._
import zio.test.Assertion.{ anything, isSubtype }

import java.util.{ List => JList, Map => JMap }
import scala.collection.JavaConverters._

object BatchWriteItemSpec extends ZIOSpecDefault {

  // -- AWS SDK helpers -------------------------------------------------------

  private def awsStr(v: String): AwsAttrValue = AwsAttrValue.builder().s(v).build()

  private val sampleAwsKey: JMap[String, AwsAttrValue]  = Map("id" -> awsStr("a")).asJava
  private val sampleAwsItem: JMap[String, AwsAttrValue] = Map("id" -> awsStr("a"), "v" -> awsStr("1")).asJava

  private def putWriteRequest(item: JMap[String, AwsAttrValue]): WriteRequest =
    WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build()

  private def deleteWriteRequest(key: JMap[String, AwsAttrValue]): WriteRequest =
    WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key).build()).build()

  private def responseWith(table: String, reqs: WriteRequest*): BatchWriteItemResponse =
    BatchWriteItemResponse
      .builder()
      .unprocessedItems(Map(table -> reqs.toList.asJava).asJava)
      .build()

  private val emptyResponse: BatchWriteItemResponse =
    BatchWriteItemResponse
      .builder()
      .unprocessedItems(Map.empty[String, JList[WriteRequest]].asJava)
      .build()

  private val notSetResponse: BatchWriteItemResponse =
    BatchWriteItemResponse.builder().build()

  // -- Stub AwsDynamoDB client -----------------------------------------------
  // Returns a canned BatchWriteItemResponse; all other methods throw (not called).

  private def stubClient(resp: BatchWriteItemResponse): AwsDynamoDB[DummyIO] =
    new AwsDynamoDB[DummyIO] {
      def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  = DummyIO.succeed(???)
      def putItem(req: PutItemRequest): DummyIO[PutItemResponse]                                  = DummyIO.succeed(???)
      def updateItem(req: UpdateItemRequest): DummyIO[UpdateItemResponse]                         = DummyIO.succeed(???)
      def deleteItem(req: DeleteItemRequest): DummyIO[DeleteItemResponse]                         = DummyIO.succeed(???)
      def batchGetItem(req: BatchGetItemRequest): DummyIO[BatchGetItemResponse]                   = DummyIO.succeed(???)
      def batchWriteItem(req: BatchWriteItemRequest): DummyIO[BatchWriteItemResponse]             = DummyIO.succeed(resp)
      def querySome(req: QueryRequest): DummyIO[QueryResponse]                                    = DummyIO.succeed(???)
      def scanSome(req: ScanRequest): DummyIO[ScanResponse]                                       = DummyIO.succeed(???)
      def createTable(req: CreateTableRequest): DummyIO[CreateTableResponse]                      = DummyIO.succeed(???)
      def deleteTable(req: DeleteTableRequest): DummyIO[DeleteTableResponse]                      = DummyIO.succeed(???)
      def describeTable(req: DescribeTableRequest): DummyIO[AwsDescribeTableResponse]             = DummyIO.succeed(???)
      def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]       = DummyIO.succeed(???)
      def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse] = DummyIO.succeed(???)
    }

  private def failingClient(t: Throwable): AwsDynamoDB[DummyIO] =
    new AwsDynamoDB[DummyIO] {
      def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  = DummyIO.succeed(???)
      def putItem(req: PutItemRequest): DummyIO[PutItemResponse]                                  = DummyIO.succeed(???)
      def updateItem(req: UpdateItemRequest): DummyIO[UpdateItemResponse]                         = DummyIO.succeed(???)
      def deleteItem(req: DeleteItemRequest): DummyIO[DeleteItemResponse]                         = DummyIO.succeed(???)
      def batchGetItem(req: BatchGetItemRequest): DummyIO[BatchGetItemResponse]                   = DummyIO.succeed(???)
      def batchWriteItem(req: BatchWriteItemRequest): DummyIO[BatchWriteItemResponse]             = DummyIO(() => throw t)
      def querySome(req: QueryRequest): DummyIO[QueryResponse]                                    = DummyIO.succeed(???)
      def scanSome(req: ScanRequest): DummyIO[ScanResponse]                                       = DummyIO.succeed(???)
      def createTable(req: CreateTableRequest): DummyIO[CreateTableResponse]                      = DummyIO.succeed(???)
      def deleteTable(req: DeleteTableRequest): DummyIO[DeleteTableResponse]                      = DummyIO.succeed(???)
      def describeTable(req: DescribeTableRequest): DummyIO[AwsDescribeTableResponse]             = DummyIO.succeed(???)
      def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]       = DummyIO.succeed(???)
      def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse] = DummyIO.succeed(???)
    }

  private def evalBatch(resp: BatchWriteItemResponse): scala.util.Try[DynamoDBQuery.BatchWriteItem.Response] = {
    val interp = new DummyIOInterpreter(stubClient(resp))
    val query  = DynamoDBQuery.batchWriteItem(List(Item("id" -> "a")))(item => DynamoDBQuery.putItem("t", item))
    scala.util.Try(interp.run(query).unsafeRun())
  }

  // -- Codec tests -----------------------------------------------------------
  // AwsCodecs.fromBatchWriteItemResponse is a pure function; no stub needed.

  private val codecSuite = suite("AwsCodecs.fromBatchWriteItemResponse")(
    test("unprocessedItems not set → Response(None)") {
      val result = AwsCodecs.fromBatchWriteItemResponse(notSetResponse)
      assertTrue(result == DynamoDBQuery.BatchWriteItem.Response(None))
    },
    test("unprocessedItems set to empty map → Response(None)") {
      val result = AwsCodecs.fromBatchWriteItemResponse(emptyResponse)
      assertTrue(result == DynamoDBQuery.BatchWriteItem.Response(None))
    },
    test("unprocessed PutRequest → Response with Put write under correct table") {
      val result = AwsCodecs.fromBatchWriteItemResponse(responseWith("t", putWriteRequest(sampleAwsItem)))
      val writes = result.unprocessedItems.flatMap(_.get("t"))
      assertTrue(
        writes.contains(
          Set[DynamoDBQuery.BatchWriteItem.Write](
            DynamoDBQuery.BatchWriteItem.Put(Item("id" -> "a", "v" -> "1"))
          )
        )
      )
    },
    test("unprocessed DeleteRequest → Response with Delete write under correct table") {
      val result = AwsCodecs.fromBatchWriteItemResponse(responseWith("t", deleteWriteRequest(sampleAwsKey)))
      val writes = result.unprocessedItems.flatMap(_.get("t"))
      assertTrue(
        writes.contains(
          Set[DynamoDBQuery.BatchWriteItem.Write](
            DynamoDBQuery.BatchWriteItem.Delete(PrimaryKey("id" -> "a"))
          )
        )
      )
    },
    test("mixed Put and Delete unprocessed → both writes present") {
      val result = AwsCodecs.fromBatchWriteItemResponse(
        responseWith("t", putWriteRequest(sampleAwsItem), deleteWriteRequest(sampleAwsKey))
      )
      val writes = result.unprocessedItems.flatMap(_.get("t"))
      assertTrue(writes.exists(_.size == 2))
    }
  )

  // -- Interpreter tests via stub client -------------------------------------
  // Exercises RealAwsInterpreter.runBatchWriteItem end-to-end:
  //   codec (toBatchWriteItemRequest / fromBatchWriteItemResponse) + response propagation.
  // Unprocessed items are a normal capacity side-effect, not an error — the response
  // is always returned successfully and callers inspect .unprocessedItems themselves.

  private val interpreterSuite = suite("DummyIOInterpreter(client) — runBatchWriteItem")(
    test("succeeds and returns Response(None) when no unprocessed items") {
      val result = evalBatch(notSetResponse)
      assertTrue(result == scala.util.Success(DynamoDBQuery.BatchWriteItem.Response(None)))
    },
    test("succeeds when unprocessedItems is an empty map") {
      val result = evalBatch(emptyResponse)
      assertTrue(result == scala.util.Success(DynamoDBQuery.BatchWriteItem.Response(None)))
    },
    test("succeeds and carries unprocessed items in Response when DynamoDB returns them") {
      val result = evalBatch(responseWith("t", putWriteRequest(sampleAwsItem)))
      assertTrue(result.isSuccess && result.get.unprocessedItems.exists(_.get("t").isDefined))
    }
  )

  private val errorPropagationSuite = suite("error propagation")(
    test("non-retryable exception from client propagates as effect-level failure") {
      val boom   = new RuntimeException("ValidationException: invalid attribute")
      val interp = new DummyIOInterpreter(failingClient(boom))
      val query  = DynamoDBQuery.batchWriteItem(List(Item("id" -> "a")))(item => DynamoDBQuery.putItem("t", item))
      val result = scala.util.Try(interp.run(query).unsafeRun())
      assertTrue(result.isFailure && (result.failed.get eq boom))
    },
    test("IO error (not a DynamoDB throttle) propagates unchanged") {
      val boom   = new java.io.IOException("connection reset")
      val interp = new DummyIOInterpreter(failingClient(boom))
      val query  = DynamoDBQuery.batchWriteItem(List(Item("id" -> "a")))(item => DynamoDBQuery.putItem("t", item))
      val result = scala.util.Try(interp.run(query).unsafeRun())
      assertTrue(result.isFailure) &&
      assert(result.failed.get)(isSubtype[java.io.IOException](anything))
    }
  )

  def spec = suite("BatchWriteItem")(codecSuite, interpreterSuite, errorPropagationSuite)
}
