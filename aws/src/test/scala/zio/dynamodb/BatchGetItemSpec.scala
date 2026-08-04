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
  DeleteTableRequest,
  DeleteTableResponse,
  DescribeTableRequest,
  DescribeTableResponse => AwsDescribeTableResponse,
  GetItemRequest,
  GetItemResponse,
  KeysAndAttributes,
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
import zio.test._
import zio.test.Assertion.{ anything, equalTo, hasField, isSubtype }

import java.util.{ List => JList, Map => JMap }
import scala.collection.JavaConverters._

object BatchGetItemSpec extends ZIOSpecDefault {

  // -- AWS SDK helpers -------------------------------------------------------

  private def awsStr(v: String): AwsAttrValue = AwsAttrValue.builder().s(v).build()

  private val sampleAwsKey: JMap[String, AwsAttrValue]  = Map("id" -> awsStr("a")).asJava
  private val sampleAwsItem: JMap[String, AwsAttrValue] = Map("id" -> awsStr("a"), "v" -> awsStr("1")).asJava

  private def responseWithItems(table: String, items: JMap[String, AwsAttrValue]*): BatchGetItemResponse =
    BatchGetItemResponse
      .builder()
      .responses(Map(table -> items.toList.asJava).asJava)
      .build()

  private def responseWithUnprocessedKeys(table: String, keys: JMap[String, AwsAttrValue]*): BatchGetItemResponse =
    BatchGetItemResponse
      .builder()
      .unprocessedKeys(
        Map(table -> KeysAndAttributes.builder().keys(keys.toList.asJava).build()).asJava
      )
      .build()

  private val emptyResponse: BatchGetItemResponse =
    BatchGetItemResponse
      .builder()
      .responses(Map.empty[String, JList[JMap[String, AwsAttrValue]]].asJava)
      .build()

  private val notSetResponse: BatchGetItemResponse =
    BatchGetItemResponse.builder().build()

  // -- Stub AwsDynamoDB client -----------------------------------------------
  // Returns a canned BatchGetItemResponse; all other methods throw (not called).

  private def stubClient(resp: BatchGetItemResponse): AwsDynamoDB[DummyIO] =
    new AwsDynamoDB[DummyIO] {
      def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  = DummyIO.succeed(???)
      def putItem(req: PutItemRequest): DummyIO[PutItemResponse]                                  = DummyIO.succeed(???)
      def updateItem(req: UpdateItemRequest): DummyIO[UpdateItemResponse]                         = DummyIO.succeed(???)
      def deleteItem(req: DeleteItemRequest): DummyIO[DeleteItemResponse]                         = DummyIO.succeed(???)
      def batchGetItem(req: BatchGetItemRequest): DummyIO[BatchGetItemResponse]                   = DummyIO.succeed(resp)
      def batchWriteItem(req: BatchWriteItemRequest): DummyIO[BatchWriteItemResponse]             = DummyIO.succeed(???)
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
      def batchGetItem(req: BatchGetItemRequest): DummyIO[BatchGetItemResponse]                   = DummyIO(() => throw t)
      def batchWriteItem(req: BatchWriteItemRequest): DummyIO[BatchWriteItemResponse]             = DummyIO.succeed(???)
      def querySome(req: QueryRequest): DummyIO[QueryResponse]                                    = DummyIO.succeed(???)
      def scanSome(req: ScanRequest): DummyIO[ScanResponse]                                       = DummyIO.succeed(???)
      def createTable(req: CreateTableRequest): DummyIO[CreateTableResponse]                      = DummyIO.succeed(???)
      def deleteTable(req: DeleteTableRequest): DummyIO[DeleteTableResponse]                      = DummyIO.succeed(???)
      def describeTable(req: DescribeTableRequest): DummyIO[AwsDescribeTableResponse]             = DummyIO.succeed(???)
      def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]       = DummyIO.succeed(???)
      def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse] = DummyIO.succeed(???)
    }

  // Unwraps the Batch.GetResult.Complete/Incomplete response — these tests exercise
  // RealAwsInterpreter.runBatchGetItem end-to-end (codec + response propagation), not the
  // response-level retry loop, so both outcomes carry a response we can inspect.
  private def evalBatch(resp: BatchGetItemResponse): scala.util.Try[DynamoDBQuery.BatchGetItem.Response] = {
    val interp = new DummyIOInterpreter(stubClient(resp))
    val query  = DynamoDBQuery.batchGetItem(List("a"))(id => DynamoDBQuery.getItem("t", PrimaryKey("id" -> id)))
    scala.util.Try(interp.run(query).unsafeRun()).map {
      case Batch.GetResult.Complete(response)   => response
      case Batch.GetResult.Incomplete(response) => response
      case other                                => throw new RuntimeException(s"expected a response, got $other")
    }
  }

  // -- Codec tests -----------------------------------------------------------
  // AwsCodecs.fromBatchGetItemResponse is a pure function; no stub needed.

  private val codecSuite = suite("AwsCodecs.fromBatchGetItemResponse")(
    test("responses not set → Response with empty responses and no unprocessed keys") {
      val result = AwsCodecs.fromBatchGetItemResponse(notSetResponse)
      assertTrue(result.responses == MapOfSet.empty[String, Item] && result.unprocessedKeys.isEmpty)
    },
    test("responses set to empty map → Response with empty responses") {
      val result = AwsCodecs.fromBatchGetItemResponse(emptyResponse)
      assertTrue(result.responses == MapOfSet.empty[String, Item] && result.unprocessedKeys.isEmpty)
    },
    test("response with items → items present under correct table") {
      val result = AwsCodecs.fromBatchGetItemResponse(responseWithItems("t", sampleAwsItem))
      val items  = result.responses.get("t")
      assertTrue(items.contains(Set[Item](Item("id" -> "a", "v" -> "1"))))
    },
    test("response with multiple items in same table → all items collected") {
      val item2  = Map("id" -> awsStr("b"), "v" -> awsStr("2")).asJava
      val result = AwsCodecs.fromBatchGetItemResponse(responseWithItems("t", sampleAwsItem, item2))
      val items  = result.responses.get("t")
      assertTrue(items.exists(_.size == 2))
    },
    test("unprocessed keys → unprocessedKeys populated with correct table and key") {
      val result = AwsCodecs.fromBatchGetItemResponse(responseWithUnprocessedKeys("t", sampleAwsKey))
      assertTrue(
        result.unprocessedKeys.contains("t") &&
          result.unprocessedKeys("t").keysSet.contains(PrimaryKey("id" -> "a"))
      )
    },
    test("no unprocessed keys → unprocessedKeys is empty") {
      val result = AwsCodecs.fromBatchGetItemResponse(responseWithItems("t", sampleAwsItem))
      assertTrue(result.unprocessedKeys.isEmpty)
    }
  )

  // -- Interpreter tests via stub client -------------------------------------
  // Exercises RealAwsInterpreter.runBatchGetItem end-to-end:
  //   codec (toBatchGetItemRequest / fromBatchGetItemResponse) + response propagation.
  // Unprocessed keys are a normal capacity side-effect, not an error — the response
  // is always returned successfully and callers inspect .unprocessedKeys themselves.

  private val interpreterSuite = suite("DummyIOInterpreter(client) — runBatchGetItem")(
    test("succeeds and returns Response with items when found") {
      val result = evalBatch(responseWithItems("t", sampleAwsItem))
      assertTrue(result.isSuccess)
    },
    test("succeeds with empty Response when nothing is set") {
      val result = evalBatch(notSetResponse)
      assertTrue(result == scala.util.Success(DynamoDBQuery.BatchGetItem.Response()))
    },
    test("succeeds and carries unprocessed keys in Response when DynamoDB returns them") {
      val result = evalBatch(responseWithUnprocessedKeys("t", sampleAwsKey))
      assertTrue(result.isSuccess && result.get.unprocessedKeys.contains("t"))
    }
  )

  // Batch queries capture effect-level failures as a Batch.GetResult.Failed value rather
  // than raising a failed effect (see Batch — errors are an inherently partial, multi-item
  // outcome, unlike single-item CRUD ops). interp.run(batchQuery) itself never fails here;
  // only a fatal (non-NonFatal) error would propagate as a failed effect.
  private val errorPropagationSuite = suite("error propagation")(
    test("non-retryable exception from client is captured as GetResult.Failed, not a failed effect") {
      val boom   = new RuntimeException("ValidationException: invalid key")
      val interp = new DummyIOInterpreter(failingClient(boom))
      val query  = DynamoDBQuery.batchGetItem(List("a"))(id => DynamoDBQuery.getItem("t", PrimaryKey("id" -> id)))
      val result = interp.run(query).unsafeRun()
      assert(result)(
        isSubtype[Batch.GetResult.Failed](hasField[Batch.GetResult.Failed, Throwable]("cause", _.cause, equalTo(boom)))
      )
    },
    test("IO error (not a DynamoDB throttle) is captured as GetResult.Failed, not a failed effect") {
      val boom   = new java.io.IOException("connection reset")
      val interp = new DummyIOInterpreter(failingClient(boom))
      val query  = DynamoDBQuery.batchGetItem(List("a"))(id => DynamoDBQuery.getItem("t", PrimaryKey("id" -> id)))
      val result = interp.run(query).unsafeRun()
      assert(result)(
        isSubtype[Batch.GetResult.Failed](hasField("cause", _.cause, isSubtype[java.io.IOException](anything)))
      )
    }
  )

  def spec = suite("BatchGetItem")(codecSuite, interpreterSuite, errorPropagationSuite)
}
