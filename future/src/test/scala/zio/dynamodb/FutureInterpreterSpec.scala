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
  Capacity => AwsCapacity,
  ConsumedCapacity => AwsConsumedCapacity,
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
  PutItemRequest,
  PutItemResponse,
  QueryRequest,
  QueryResponse,
  ReturnConsumedCapacity,
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
import zio.test.Assertion.{ anything, isSubtype }

import scala.collection.JavaConverters._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object FutureInterpreterSpec extends ZIOSpecDefault {

  // -- Test helpers ----------------------------------------------------------

  private def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  private def mkInterceptor(): (ResponseInterceptor[Future], () => List[DynamoDBResponseMetadata]) = {
    var captured: List[DynamoDBResponseMetadata] = Nil
    val interceptor                              = new ResponseInterceptor[Future] {
      def onResponse(meta: DynamoDBResponseMetadata): Future[Unit] =
        Future.successful { captured = captured :+ meta }
    }
    (interceptor, () => captured)
  }

  private val futureOps = new EffectOps[Future] {
    def map[A, B](fa: Future[A])(f: A => B): Future[B]             = fa.map(f)
    def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
  }

  // -- AWS SDK value builders ------------------------------------------------

  private def awsStr(v: String): AwsAttrValue = AwsAttrValue.builder().s(v).build()

  private val awsKey: java.util.Map[String, AwsAttrValue] =
    Map("id" -> awsStr("alice")).asJava

  private val awsCap: AwsConsumedCapacity =
    AwsConsumedCapacity.builder().tableName("t").readCapacityUnits(0.5).build()

  // -- Full stub client ------------------------------------------------------

  private def fullStub(
    onGet: GetItemRequest => GetItemResponse = _ => ???,
    onPut: PutItemRequest => PutItemResponse = _ => ???,
    onUpdate: UpdateItemRequest => UpdateItemResponse = _ => ???,
    onDelete: DeleteItemRequest => DeleteItemResponse = _ => ???,
    onQuery: QueryRequest => QueryResponse = _ => ???,
    onScan: ScanRequest => ScanResponse = _ => ???,
    onBatchGet: BatchGetItemRequest => BatchGetItemResponse = _ => ???,
    onBatchWrite: BatchWriteItemRequest => BatchWriteItemResponse = _ => ???,
    onCreate: CreateTableRequest => CreateTableResponse = _ => ???,
    onDeleteTable: DeleteTableRequest => DeleteTableResponse = _ => ???,
    onDescribe: DescribeTableRequest => AwsDescribeTableResponse = _ => ???
  ): AwsDynamoDB[Future] = new AwsDynamoDB[Future] {
    def getItem(req: GetItemRequest): Future[GetItemResponse]                                  = Future.successful(onGet(req))
    def putItem(req: PutItemRequest): Future[PutItemResponse]                                  = Future.successful(onPut(req))
    def updateItem(req: UpdateItemRequest): Future[UpdateItemResponse]                         = Future.successful(onUpdate(req))
    def deleteItem(req: DeleteItemRequest): Future[DeleteItemResponse]                         = Future.successful(onDelete(req))
    def query(req: QueryRequest): Future[QueryResponse]                                        = Future.successful(onQuery(req))
    def scan(req: ScanRequest): Future[ScanResponse]                                           = Future.successful(onScan(req))
    def batchGetItem(req: BatchGetItemRequest): Future[BatchGetItemResponse]                   = Future.successful(onBatchGet(req))
    def batchWriteItem(req: BatchWriteItemRequest): Future[BatchWriteItemResponse]             =
      Future.successful(onBatchWrite(req))
    def createTable(req: CreateTableRequest): Future[CreateTableResponse]                      = Future.successful(onCreate(req))
    def deleteTable(req: DeleteTableRequest): Future[DeleteTableResponse]                      = Future.successful(onDeleteTable(req))
    def describeTable(req: DescribeTableRequest): Future[AwsDescribeTableResponse]             = Future.successful(onDescribe(req))
    def transactGetItems(req: TransactGetItemsRequest): Future[TransactGetItemsResponse]       =
      Future.failed(new NotImplementedError())
    def transactWriteItems(req: TransactWriteItemsRequest): Future[TransactWriteItemsResponse] =
      Future.failed(new NotImplementedError())
  }

  // -- getItem ---------------------------------------------------------------

  private val getItemSuite = suite("getItem")(
    test("fires interceptor with GetItem metadata and correct tableName") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = GetItemResponse.builder().consumedCapacity(awsCap).build()
      val req                     = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.getItem(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.GetItem] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
            m.tableName == "t"
          }
      )
    },
    test("populates correlation.primaryKey for getItem") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = GetItemResponse.builder().consumedCapacity(awsCap).build()
      val req                     = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.getItem(req))
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(m.correlation.primaryKey.isDefined)
    },
    test("consumed is None when response has no consumedCapacity") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = GetItemResponse.builder().build()
      val req                     = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.getItem(req))
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(m.consumed.isEmpty)
    },
    test("enriched request has ReturnConsumedCapacity.TOTAL") {
      val (interceptor, _)            = mkInterceptor()
      var receivedReq: GetItemRequest = null
      val resp                        = GetItemResponse.builder().build()
      val stub                        = fullStub(onGet = { req => receivedReq = req; resp })
      val sut                         = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      val req                         = GetItemRequest.builder().tableName("t").key(awsKey).build()
      await(sut.getItem(req))
      assertTrue(receivedReq.returnConsumedCapacity() == ReturnConsumedCapacity.TOTAL)
    },
    test("returns the original response after interception") {
      val (interceptor, _) = mkInterceptor()
      val item             = Map("id" -> awsStr("alice")).asJava
      val resp             = GetItemResponse.builder().item(item).consumedCapacity(awsCap).build()
      val req              = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub             = fullStub(onGet = _ => resp)
      val sut              = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      val result           = await(sut.getItem(req))
      assertTrue(result eq resp)
    }
  )

  // -- putItem ---------------------------------------------------------------

  private val putItemSuite = suite("putItem")(
    test("fires interceptor with PutItem metadata and correct tableName") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = PutItemResponse.builder().consumedCapacity(awsCap).build()
      val item                    = Map("id" -> awsStr("alice"), "v" -> awsStr("1")).asJava
      val req                     = PutItemRequest.builder().tableName("t").item(item).build()
      val stub                    = fullStub(onPut = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.putItem(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.PutItem] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.PutItem]
            m.tableName == "t"
          }
      )
    },
    test("correlation.primaryKey is None for putItem") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = PutItemResponse.builder().build()
      val item                    = Map("id" -> awsStr("alice")).asJava
      val req                     = PutItemRequest.builder().tableName("t").item(item).build()
      val stub                    = fullStub(onPut = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.putItem(req))
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.PutItem]
      assertTrue(m.correlation.primaryKey.isEmpty)
    }
  )

  // -- updateItem ------------------------------------------------------------

  private val updateItemSuite = suite("updateItem")(
    test("fires interceptor with UpdateItem metadata and correct tableName") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = UpdateItemResponse.builder().consumedCapacity(awsCap).build()
      val req                     = UpdateItemRequest
        .builder()
        .tableName("t")
        .key(awsKey)
        .updateExpression("SET v = :v")
        .expressionAttributeValues(Map(":v" -> awsStr("x")).asJava)
        .build()
      val stub                    = fullStub(onUpdate = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.updateItem(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.UpdateItem] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.UpdateItem]
            m.tableName == "t"
          }
      )
    },
    test("populates correlation.primaryKey for updateItem") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = UpdateItemResponse.builder().build()
      val req                     = UpdateItemRequest
        .builder()
        .tableName("t")
        .key(awsKey)
        .updateExpression("SET v = :v")
        .expressionAttributeValues(Map(":v" -> awsStr("x")).asJava)
        .build()
      val stub                    = fullStub(onUpdate = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.updateItem(req))
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.UpdateItem]
      assertTrue(m.correlation.primaryKey.isDefined)
    }
  )

  // -- deleteItem ------------------------------------------------------------

  private val deleteItemSuite = suite("deleteItem")(
    test("fires interceptor with DeleteItem metadata and correct tableName") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = DeleteItemResponse.builder().consumedCapacity(awsCap).build()
      val req                     = DeleteItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onDelete = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.deleteItem(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.DeleteItem] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.DeleteItem]
            m.tableName == "t"
          }
      )
    },
    test("populates correlation.primaryKey for deleteItem") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = DeleteItemResponse.builder().build()
      val req                     = DeleteItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onDelete = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.deleteItem(req))
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.DeleteItem]
      assertTrue(m.correlation.primaryKey.isDefined)
    }
  )

  // -- query -------------------------------------------------------------

  private val queryItemSuite = suite("query")(
    test("fires interceptor with Query metadata and correct tableName") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = QueryResponse.builder().consumedCapacity(awsCap).build()
      val req                     = QueryRequest.builder().tableName("t").build()
      val stub                    = fullStub(onQuery = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.query(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.Query] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.Query]
            m.tableName == "t"
          }
      )
    }
  )

  // -- scan --------------------------------------------------------------

  private val scanItemSuite = suite("scan")(
    test("fires interceptor with Scan metadata and correct tableName") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = ScanResponse.builder().consumedCapacity(awsCap).build()
      val req                     = ScanRequest.builder().tableName("t").build()
      val stub                    = fullStub(onScan = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.scan(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.Scan] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.Scan]
            m.tableName == "t"
          }
      )
    }
  )

  // -- batchGetItem ----------------------------------------------------------

  private val batchGetItemSuite = suite("batchGetItem")(
    test("fires interceptor with BatchGetItem metadata containing consumed chunk") {
      val (interceptor, captured) = mkInterceptor()
      val cap                     = AwsConsumedCapacity.builder().tableName("t").readCapacityUnits(1.0).build()
      val resp                    = BatchGetItemResponse
        .builder()
        .consumedCapacity(java.util.Arrays.asList(cap))
        .build()
      val req                     = BatchGetItemRequest
        .builder()
        .requestItems(Map.empty[String, software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes].asJava)
        .build()
      val stub                    = fullStub(onBatchGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.batchGetItem(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.BatchGetItem] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.BatchGetItem]
            m.consumed.length == 1
          }
      )
    },
    test("batchGetItem consumed is empty when response has no consumed capacity") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = BatchGetItemResponse.builder().build()
      val req                     = BatchGetItemRequest
        .builder()
        .requestItems(Map.empty[String, software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes].asJava)
        .build()
      val stub                    = fullStub(onBatchGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.batchGetItem(req))
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.BatchGetItem]
      assertTrue(m.consumed.isEmpty)
    }
  )

  // -- batchWriteItem --------------------------------------------------------

  private val batchWriteItemSuite = suite("batchWriteItem")(
    test("fires interceptor with BatchWriteItem metadata containing consumed chunk") {
      val (interceptor, captured) = mkInterceptor()
      val cap                     = AwsConsumedCapacity.builder().tableName("t").writeCapacityUnits(2.0).build()
      val resp                    = BatchWriteItemResponse
        .builder()
        .consumedCapacity(java.util.Arrays.asList(cap))
        .build()
      val req                     = BatchWriteItemRequest
        .builder()
        .requestItems(
          Map.empty[String, java.util.List[software.amazon.awssdk.services.dynamodb.model.WriteRequest]].asJava
        )
        .build()
      val stub                    = fullStub(onBatchWrite = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.batchWriteItem(req))
      val metas                   = captured()
      assertTrue(
        metas.length == 1 &&
          metas.head.isInstanceOf[DynamoDBResponseMetadata.BatchWriteItem] && {
            val m = metas.head.asInstanceOf[DynamoDBResponseMetadata.BatchWriteItem]
            m.consumed.length == 1
          }
      )
    },
    test("batchWriteItem consumed is empty when response has no consumed capacity") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = BatchWriteItemResponse.builder().build()
      val req                     = BatchWriteItemRequest
        .builder()
        .requestItems(
          Map.empty[String, java.util.List[software.amazon.awssdk.services.dynamodb.model.WriteRequest]].asJava
        )
        .build()
      val stub                    = fullStub(onBatchWrite = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.batchWriteItem(req))
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.BatchWriteItem]
      assertTrue(m.consumed.isEmpty)
    }
  )

  // -- DDL operations do NOT fire the interceptor ----------------------------

  private val ddlSuite = suite("DDL operations do not fire interceptor")(
    test("createTable does not invoke interceptor") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = CreateTableResponse.builder().build()
      val req                     = CreateTableRequest.builder().tableName("t").build()
      val stub                    = fullStub(onCreate = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.createTable(req))
      assertTrue(captured().isEmpty)
    },
    test("deleteTable does not invoke interceptor") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = DeleteTableResponse.builder().build()
      val req                     = DeleteTableRequest.builder().tableName("t").build()
      val stub                    = fullStub(onDeleteTable = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.deleteTable(req))
      assertTrue(captured().isEmpty)
    },
    test("describeTable does not invoke interceptor") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = AwsDescribeTableResponse.builder().build()
      val req                     = DescribeTableRequest.builder().tableName("t").build()
      val stub                    = fullStub(onDescribe = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[Future](stub, interceptor, futureOps)
      await(sut.describeTable(req))
      assertTrue(captured().isEmpty)
    }
  )

  // -- fail / absolve error-type contract ------------------------------------

  private val errorTypeSuite = suite("fail and absolve error types")(
    test("fail raises DynamoDBError directly, not wrapped in RuntimeException") {
      val interp = new FutureInterpreter(fullStub())
      val error  = DynamoDBError.ItemError.ValueNotFound("missing")
      val result = scala.util.Try(await(interp.run(DynamoDBQuery.fail(error))))
      assert(result.failed.get)(isSubtype[DynamoDBError](anything))
    },
    test("absolve on Left raises DynamoDBError directly, not wrapped in RuntimeException") {
      val interp = new FutureInterpreter(fullStub())
      val error  = DynamoDBError.ItemError.ValueNotFound("missing")
      val q      = DynamoDBQuery.Absolve(DynamoDBQuery(Left(error): Either[DynamoDBError.ItemError, Int]))
      val result = scala.util.Try(await(interp.run(q)))
      assert(result.failed.get)(isSubtype[DynamoDBError](anything))
    }
  )

  // -- effect primitives (sleep/attempt/raiseError) — used internally by the
  // retry machinery; not exercised by any query since this module has no
  // dedicated retry spec (unlike zio's RetrySpec.scala).
  private val effectPrimitivesSuite = suite("effect primitives")(
    test("sleep completes after the given duration without blocking") {
      val interp = new FutureInterpreter(fullStub())
      await(interp.sleep(scala.concurrent.duration.Duration.Zero))
      assertTrue(true)
    },
    test("attempt wraps a successful Future in Right") {
      val interp = new FutureInterpreter(fullStub())
      assertTrue(await(interp.attempt(Future.successful(42))) == Right(42))
    },
    test("attempt wraps a failed Future in Left") {
      val interp = new FutureInterpreter(fullStub())
      val boom   = new RuntimeException("boom")
      assertTrue(await(interp.attempt(Future.failed(boom))) == Left(boom))
    },
    test("raiseError produces a failed Future with the given throwable") {
      val interp = new FutureInterpreter(fullStub())
      val boom   = new RuntimeException("boom")
      val result = scala.util.Try(await(interp.raiseError(boom)))
      assertTrue(result.failed.get eq boom)
    }
  )

  // -- FutureResponseInterceptor.accumulating --------------------------------

  private val accumulatorSuite = suite("FutureResponseInterceptor.accumulating")(
    test("collects metadata entries in call order") {
      val acc   = await(FutureResponseInterceptor.accumulating)
      val m1    = DynamoDBResponseMetadata.GetItem(
        tableName = "t",
        consumed = None,
        correlation = CorrelationContext(primaryKey = None)
      )
      val m2    = DynamoDBResponseMetadata.PutItem(
        tableName = "t",
        consumed = None,
        collectionMetrics = None,
        correlation = CorrelationContext(primaryKey = None)
      )
      await(acc.interceptor.onResponse(m1))
      await(acc.interceptor.onResponse(m2))
      val chunk = acc.results()
      assertTrue(
        chunk.length == 2 &&
          chunk(0).isInstanceOf[DynamoDBResponseMetadata.GetItem] &&
          chunk(1).isInstanceOf[DynamoDBResponseMetadata.PutItem]
      )
    },
    test("results is non-destructive — can be called multiple times") {
      val acc    = await(FutureResponseInterceptor.accumulating)
      val m      = DynamoDBResponseMetadata.GetItem(
        tableName = "t",
        consumed = None,
        correlation = CorrelationContext(primaryKey = None)
      )
      await(acc.interceptor.onResponse(m))
      val first  = acc.results()
      val second = acc.results()
      assertTrue(first.length == 1 && second.length == 1)
    },
    test("fresh accumulator per call starts empty") {
      val acc   = await(FutureResponseInterceptor.accumulating)
      val chunk = acc.results()
      assertTrue(chunk.isEmpty)
    }
  )

  def spec = suite("FutureInterpreter")(
    getItemSuite,
    putItemSuite,
    updateItemSuite,
    deleteItemSuite,
    queryItemSuite,
    scanItemSuite,
    batchGetItemSuite,
    batchWriteItemSuite,
    ddlSuite,
    accumulatorSuite,
    errorTypeSuite,
    effectPrimitivesSuite
  )
}
