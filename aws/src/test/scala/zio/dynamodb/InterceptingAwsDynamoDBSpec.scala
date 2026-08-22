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
  ItemCollectionMetrics => AwsItemCollectionMetrics,
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

import scala.collection.JavaConverters._

object InterceptingAwsDynamoDBSpec extends ZIOSpecDefault {

  // -- Test helpers ----------------------------------------------------------

  private def mkInterceptor(): (ResponseInterceptor[DummyIO], () => List[DynamoDBResponseMetadata]) = {
    var captured: List[DynamoDBResponseMetadata] = Nil
    val interceptor                              = new ResponseInterceptor[DummyIO] {
      def onResponse(meta: DynamoDBResponseMetadata): DummyIO[Unit] =
        DummyIO.succeed { captured = captured :+ meta }
    }
    (interceptor, () => captured)
  }

  private val dummyOps = new EffectOps[DummyIO] {
    def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              =
      DummyIO(() => f(fa.unsafeRun()))
    def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] =
      DummyIO(() => f(fa.unsafeRun()).unsafeRun())
  }

  // -- AWS SDK value builders ------------------------------------------------

  private def awsStr(v: String): AwsAttrValue = AwsAttrValue.builder().s(v).build()

  private val awsKey: java.util.Map[String, AwsAttrValue] =
    Map("id" -> awsStr("alice")).asJava

  private val awsCap: AwsConsumedCapacity =
    AwsConsumedCapacity.builder().tableName("t").readCapacityUnits(0.5).build()

  // -- Full stub client that throws for any method not under test -----------

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
  ): AwsDynamoDB[DummyIO] = new AwsDynamoDB[DummyIO] {
    def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  = DummyIO.succeed(onGet(req))
    def putItem(req: PutItemRequest): DummyIO[PutItemResponse]                                  = DummyIO.succeed(onPut(req))
    def updateItem(req: UpdateItemRequest): DummyIO[UpdateItemResponse]                         = DummyIO.succeed(onUpdate(req))
    def deleteItem(req: DeleteItemRequest): DummyIO[DeleteItemResponse]                         = DummyIO.succeed(onDelete(req))
    def query(req: QueryRequest): DummyIO[QueryResponse]                                        = DummyIO.succeed(onQuery(req))
    def scan(req: ScanRequest): DummyIO[ScanResponse]                                           = DummyIO.succeed(onScan(req))
    def batchGetItem(req: BatchGetItemRequest): DummyIO[BatchGetItemResponse]                   = DummyIO.succeed(onBatchGet(req))
    def batchWriteItem(req: BatchWriteItemRequest): DummyIO[BatchWriteItemResponse]             = DummyIO.succeed(onBatchWrite(req))
    def createTable(req: CreateTableRequest): DummyIO[CreateTableResponse]                      = DummyIO.succeed(onCreate(req))
    def deleteTable(req: DeleteTableRequest): DummyIO[DeleteTableResponse]                      = DummyIO.succeed(onDeleteTable(req))
    def describeTable(req: DescribeTableRequest): DummyIO[AwsDescribeTableResponse]             = DummyIO.succeed(onDescribe(req))
    def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]       = DummyIO.succeed(???)
    def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse] = DummyIO.succeed(???)
  }

  // -- getItem ---------------------------------------------------------------

  private val getItemSuite = suite("getItem")(
    test("fires interceptor with GetItem metadata and correct tableName") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = GetItemResponse.builder().consumedCapacity(awsCap).build()
      val req                     = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.getItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.getItem(req).unsafeRun()
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(m.correlation.primaryKey.isDefined)
    },
    test("consumed is None when response has no consumedCapacity") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = GetItemResponse.builder().build() // no consumed capacity
      val req                     = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.getItem(req).unsafeRun()
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(m.consumed.isEmpty)
    },
    test("enriched request has ReturnConsumedCapacity.TOTAL") {
      val (interceptor, _)            = mkInterceptor()
      var receivedReq: GetItemRequest = null
      val resp                        = GetItemResponse.builder().build()
      val stub                        = fullStub(onGet = { req => receivedReq = req; resp })
      val sut                         = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      val req                         = GetItemRequest.builder().tableName("t").key(awsKey).build()
      sut.getItem(req).unsafeRun()
      assertTrue(receivedReq.returnConsumedCapacity() == ReturnConsumedCapacity.TOTAL)
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.putItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.putItem(req).unsafeRun()
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.PutItem]
      assertTrue(m.correlation.primaryKey.isEmpty)
    },
    test("collectionMetrics is populated when the response has itemCollectionMetrics") {
      val (interceptor, captured) = mkInterceptor()
      val metrics                 = AwsItemCollectionMetrics
        .builder()
        .itemCollectionKey(awsKey)
        .sizeEstimateRangeGB(List(java.lang.Double.valueOf(1.0), java.lang.Double.valueOf(2.0)).asJava)
        .build()
      val resp                    = PutItemResponse.builder().itemCollectionMetrics(metrics).build()
      val item                    = Map("id" -> awsStr("alice")).asJava
      val req                     = PutItemRequest.builder().tableName("t").item(item).build()
      val stub                    = fullStub(onPut = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.putItem(req).unsafeRun()
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.PutItem]
      assertTrue(
        m.collectionMetrics.exists(_.itemCollectionKey.contains(AttrMap("id" -> "alice"))),
        m.collectionMetrics.exists(_.sizeEstimateRangeGB == ((1.0, 2.0)))
      )
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.updateItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.updateItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.deleteItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.deleteItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.query(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.scan(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.batchGetItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.batchGetItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.batchWriteItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.batchWriteItem(req).unsafeRun()
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
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.createTable(req).unsafeRun()
      assertTrue(captured().isEmpty)
    },
    test("deleteTable does not invoke interceptor") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = DeleteTableResponse.builder().build()
      val req                     = DeleteTableRequest.builder().tableName("t").build()
      val stub                    = fullStub(onDeleteTable = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.deleteTable(req).unsafeRun()
      assertTrue(captured().isEmpty)
    },
    test("describeTable does not invoke interceptor") {
      val (interceptor, captured) = mkInterceptor()
      val resp                    = AwsDescribeTableResponse.builder().build()
      val req                     = DescribeTableRequest.builder().tableName("t").build()
      val stub                    = fullStub(onDescribe = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.describeTable(req).unsafeRun()
      assertTrue(captured().isEmpty)
    }
  )

  // -- LSI / GSI index capacity breakdown ------------------------------------

  private val indexCapacitySuite = suite("index capacity breakdown")(
    test("localSecondaryIndexes populated when AWS returns LSI capacity") {
      val (interceptor, captured) = mkInterceptor()
      val lsiCap                  = AwsCapacity.builder().readCapacityUnits(0.5).capacityUnits(0.5).build()
      val cap                     = AwsConsumedCapacity
        .builder()
        .tableName("t")
        .readCapacityUnits(1.0)
        .localSecondaryIndexes(Map("my-lsi" -> lsiCap).asJava)
        .build()
      val resp                    = GetItemResponse.builder().consumedCapacity(cap).build()
      val req                     = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.getItem(req).unsafeRun()
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(
        m.consumed.exists(_.localSecondaryIndexes.contains("my-lsi")) &&
          m.consumed.exists(_.localSecondaryIndexes("my-lsi").readCapacityUnits.contains(0.5))
      )
    },
    test("globalSecondaryIndexes populated when AWS returns GSI capacity") {
      val (interceptor, captured) = mkInterceptor()
      val gsiCap                  = AwsCapacity.builder().readCapacityUnits(2.0).capacityUnits(2.0).build()
      val cap                     = AwsConsumedCapacity
        .builder()
        .tableName("t")
        .readCapacityUnits(2.0)
        .globalSecondaryIndexes(Map("my-gsi" -> gsiCap).asJava)
        .build()
      val resp                    = QueryResponse.builder().consumedCapacity(cap).build()
      val req                     = QueryRequest.builder().tableName("t").build()
      val stub                    = fullStub(onQuery = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.query(req).unsafeRun()
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.Query]
      assertTrue(
        m.consumed.exists(_.globalSecondaryIndexes.contains("my-gsi")) &&
          m.consumed.exists(_.globalSecondaryIndexes("my-gsi").readCapacityUnits.contains(2.0))
      )
    },
    test("localSecondaryIndexes and globalSecondaryIndexes are empty maps when not present") {
      val (interceptor, captured) = mkInterceptor()
      val cap                     = AwsConsumedCapacity.builder().tableName("t").readCapacityUnits(0.5).build()
      val resp                    = GetItemResponse.builder().consumedCapacity(cap).build()
      val req                     = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub                    = fullStub(onGet = _ => resp)
      val sut                     = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      sut.getItem(req).unsafeRun()
      val m                       = captured().head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(
        m.consumed.exists(_.localSecondaryIndexes.isEmpty) &&
          m.consumed.exists(_.globalSecondaryIndexes.isEmpty)
      )
    }
  )

  // -- Return value is unchanged after interception -------------------------

  private val returnValueSuite = suite("return value is passed through")(
    test("getItem returns the original response after interception") {
      val (interceptor, _) = mkInterceptor()
      val item             = Map("id" -> awsStr("alice")).asJava
      val resp             = GetItemResponse.builder().item(item).consumedCapacity(awsCap).build()
      val req              = GetItemRequest.builder().tableName("t").key(awsKey).build()
      val stub             = fullStub(onGet = _ => resp)
      val sut              = new InterceptingAwsDynamoDB[DummyIO](stub, interceptor, dummyOps)
      val result           = sut.getItem(req).unsafeRun()
      assertTrue(result eq resp)
    }
  )

  def spec = suite("InterceptingAwsDynamoDB")(
    getItemSuite,
    putItemSuite,
    updateItemSuite,
    deleteItemSuite,
    queryItemSuite,
    scanItemSuite,
    batchGetItemSuite,
    batchWriteItemSuite,
    ddlSuite,
    indexCapacitySuite,
    returnValueSuite
  )
}
