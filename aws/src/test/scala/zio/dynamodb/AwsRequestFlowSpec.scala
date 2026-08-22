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
  ReturnConsumedCapacity => AwsCapacity,
  ReturnItemCollectionMetrics => AwsItemMetrics,
  ReturnValue => AwsReturnValue,
  ScanRequest,
  ScanResponse,
  Select => AwsSelect,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  UpdateItemRequest,
  UpdateItemResponse
}
import zio.dynamodb.{ ReturnConsumedCapacity => LibCapacity }
import zio.test._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Tests the full flow: DynamoDBQuery builder methods → interpreter runAny →
 * AWS request, and (for capacity) the same flow with an interceptor acting as
 * a logger to capture ConsumedCapacity from the response.
 *
 * These are distinct from AwsCodecsCapacitySpec / AwsCodecsMetadataSpec, which
 * test the codec layer directly.  Here the query runs through
 * DummyIOInterpreter.run(q) so the interpreter's runAny dispatch is exercised.
 *
 * Note: consistency, returns, and metrics suites use a plain
 * DummyIOInterpreter (no InterceptingAwsDynamoDB) so the request the stub
 * receives is exactly what AwsCodecs built — the interceptor must not override
 * these fields before the stub sees them.
 */
object AwsRequestFlowSpec extends ZIOSpecDefault {

  private val pk    = PrimaryKey("id" -> "a")
  private val item  = Item("id" -> "a")
  private val table = "t"

  private val awsConsumedCap: AwsConsumedCapacity =
    AwsConsumedCapacity.builder().tableName(table).readCapacityUnits(0.5).build()

  // ---------------------------------------------------------------------------
  // Generic capturing stub builder
  // ---------------------------------------------------------------------------

  private def stub(
    onGetItem: GetItemRequest => GetItemResponse = _ => GetItemResponse.builder().build(),
    onPutItem: PutItemRequest => PutItemResponse = _ => PutItemResponse.builder().build(),
    onDeleteItem: DeleteItemRequest => DeleteItemResponse = _ => DeleteItemResponse.builder().build(),
    onQuery: QueryRequest => QueryResponse = _ => QueryResponse.builder().build(),
    onScan: ScanRequest => ScanResponse = _ => ScanResponse.builder().build(),
    onBatchWrite: BatchWriteItemRequest => BatchWriteItemResponse = _ => BatchWriteItemResponse.builder().build(),
    onCreateTable: CreateTableRequest => CreateTableResponse = _ => CreateTableResponse.builder().build()
  ): AwsDynamoDB[DummyIO] =
    new AwsDynamoDB[DummyIO] {
      def getItem(req: GetItemRequest): DummyIO[GetItemResponse]                                  = DummyIO.succeed(onGetItem(req))
      def putItem(req: PutItemRequest): DummyIO[PutItemResponse]                                  = DummyIO.succeed(onPutItem(req))
      def deleteItem(req: DeleteItemRequest): DummyIO[DeleteItemResponse]                         = DummyIO.succeed(onDeleteItem(req))
      def query(req: QueryRequest): DummyIO[QueryResponse]                                        = DummyIO.succeed(onQuery(req))
      def scan(req: ScanRequest): DummyIO[ScanResponse]                                           = DummyIO.succeed(onScan(req))
      def batchWriteItem(req: BatchWriteItemRequest): DummyIO[BatchWriteItemResponse]             =
        DummyIO.succeed(onBatchWrite(req))
      def createTable(req: CreateTableRequest): DummyIO[CreateTableResponse]                      = DummyIO.succeed(onCreateTable(req))
      def updateItem(req: UpdateItemRequest): DummyIO[UpdateItemResponse]                         = DummyIO.succeed(???)
      def batchGetItem(req: BatchGetItemRequest): DummyIO[BatchGetItemResponse]                   = DummyIO.succeed(???)
      def deleteTable(req: DeleteTableRequest): DummyIO[DeleteTableResponse]                      = DummyIO.succeed(???)
      def describeTable(req: DescribeTableRequest): DummyIO[AwsDescribeTableResponse]             = DummyIO.succeed(???)
      def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]       = DummyIO.succeed(???)
      def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse] = DummyIO.succeed(???)
    }

  private val dummyOps = new EffectOps[DummyIO] {
    def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              = DummyIO(() => f(fa.unsafeRun()))
    def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] = DummyIO(() => f(fa.unsafeRun()).unsafeRun())
  }

  // ---------------------------------------------------------------------------
  // Suite 1: capacity flows through interpreter to AWS request
  // ---------------------------------------------------------------------------

  private val capacityRequestSuite = suite("capacity flows through interpreter to AWS request")(
    test("capacity(Total) produces returnConsumedCapacity TOTAL in the request") {
      var captured: GetItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onGetItem = req => { captured = req; GetItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.getItem(table, pk).capacity(LibCapacity.Total)).unsafeRun()
      assertTrue(captured.returnConsumedCapacity() == AwsCapacity.TOTAL)
    },
    test("capacity(Indexes) produces returnConsumedCapacity INDEXES in the request") {
      var captured: GetItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onGetItem = req => { captured = req; GetItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.getItem(table, pk).capacity(LibCapacity.Indexes)).unsafeRun()
      assertTrue(captured.returnConsumedCapacity() == AwsCapacity.INDEXES)
    },
    test("no capacity set produces returnConsumedCapacity NONE in the request") {
      var captured: GetItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onGetItem = req => { captured = req; GetItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.getItem(table, pk)).unsafeRun()
      assertTrue(captured.returnConsumedCapacity() == AwsCapacity.NONE)
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 2: interceptor captures ConsumedCapacity from response
  // ---------------------------------------------------------------------------

  private val capacityInterceptorSuite = suite("interceptor captures ConsumedCapacity from response")(
    test("consumed capacity is Some when stub returns it") {
      val log    = ListBuffer[DynamoDBResponseMetadata]()
      val logger = new ResponseInterceptor[DummyIO] {
        def onResponse(meta: DynamoDBResponseMetadata): DummyIO[Unit] = DummyIO.succeed(log += meta)
      }
      val resp   = GetItemResponse.builder().consumedCapacity(awsConsumedCap).build()
      val client = new InterceptingAwsDynamoDB(stub(onGetItem = _ => resp), logger, dummyOps)
      val interp = new DummyIOInterpreter(client)
      interp.run(DynamoDBQuery.getItem(table, pk).capacity(LibCapacity.Total)).unsafeRun()
      val meta   = log.head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(
        log.size == 1 &&
          meta.consumed.isDefined &&
          meta.consumed.exists(_.readCapacityUnits.contains(0.5))
      )
    },
    test("consumed capacity is None when stub returns no consumed capacity") {
      val log    = ListBuffer[DynamoDBResponseMetadata]()
      val logger = new ResponseInterceptor[DummyIO] {
        def onResponse(meta: DynamoDBResponseMetadata): DummyIO[Unit] = DummyIO.succeed(log += meta)
      }
      val client = new InterceptingAwsDynamoDB(stub(), logger, dummyOps)
      val interp = new DummyIOInterpreter(client)
      interp.run(DynamoDBQuery.getItem(table, pk)).unsafeRun()
      val meta   = log.head.asInstanceOf[DynamoDBResponseMetadata.GetItem]
      assertTrue(log.size == 1 && meta.consumed.isEmpty)
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 3: consistency flows through interpreter to AWS request
  // ---------------------------------------------------------------------------

  private val consistencyRequestSuite = suite("consistency flows through interpreter to AWS request")(
    test("getItem consistency(Strong) produces consistentRead true") {
      var captured: GetItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onGetItem = req => { captured = req; GetItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.getItem(table, pk).consistency(ConsistencyMode.Strong)).unsafeRun()
      assertTrue(captured.consistentRead() == true)
    },
    test("getItem consistency(Weak) produces consistentRead false") {
      var captured: GetItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onGetItem = req => { captured = req; GetItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.getItem(table, pk).consistency(ConsistencyMode.Weak)).unsafeRun()
      assertTrue(captured.consistentRead() == false)
    },
    test("query consistency(Strong) produces consistentRead true") {
      var captured: QueryRequest = null
      val interp                 = new DummyIOInterpreter(stub(onQuery = req => { captured = req; QueryResponse.builder().build() }))
      interp.run(DynamoDBQuery.query(table, 10).consistency(ConsistencyMode.Strong)).unsafeRun()
      assertTrue(captured.consistentRead() == true)
    },
    test("scan consistency(Strong) produces consistentRead true") {
      var captured: ScanRequest = null
      val interp                = new DummyIOInterpreter(stub(onScan = req => { captured = req; ScanResponse.builder().build() }))
      interp.run(DynamoDBQuery.scan(table, 10).consistency(ConsistencyMode.Strong)).unsafeRun()
      assertTrue(captured.consistentRead() == true)
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 4: returns flows through interpreter to AWS request
  // ---------------------------------------------------------------------------

  private val returnsRequestSuite = suite("returns flows through interpreter to AWS request")(
    test("putItem returns(AllOld) produces returnValues ALL_OLD in the request") {
      var captured: PutItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onPutItem = req => { captured = req; PutItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.putItem(table, item).returns(ReturnValues.AllOld)).unsafeRun()
      assertTrue(captured.returnValues() == AwsReturnValue.ALL_OLD)
    },
    test("putItem no returns set produces returnValues NONE in the request") {
      var captured: PutItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onPutItem = req => { captured = req; PutItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.putItem(table, item)).unsafeRun()
      assertTrue(captured.returnValues() == AwsReturnValue.NONE)
    },
    test("deleteItem returns(AllOld) produces returnValues ALL_OLD in the request") {
      var captured: DeleteItemRequest = null
      val interp                      = new DummyIOInterpreter(stub(onDeleteItem = req => {
        captured = req; DeleteItemResponse.builder().build()
      }))
      interp.run(DynamoDBQuery.deleteItem(table, pk).returns(ReturnValues.AllOld)).unsafeRun()
      assertTrue(captured.returnValues() == AwsReturnValue.ALL_OLD)
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 5: metrics flows through interpreter to AWS request
  // ---------------------------------------------------------------------------

  private val metricsRequestSuite = suite("metrics flows through interpreter to AWS request")(
    test("putItem metrics(Size) produces returnItemCollectionMetrics SIZE in the request") {
      var captured: PutItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onPutItem = req => { captured = req; PutItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.putItem(table, item).metrics(ReturnItemCollectionMetrics.Size)).unsafeRun()
      assertTrue(captured.returnItemCollectionMetrics() == AwsItemMetrics.SIZE)
    },
    test("putItem no metrics set produces returnItemCollectionMetrics NONE in the request") {
      var captured: PutItemRequest = null
      val interp                   =
        new DummyIOInterpreter(stub(onPutItem = req => { captured = req; PutItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.putItem(table, item)).unsafeRun()
      assertTrue(captured.returnItemCollectionMetrics() == AwsItemMetrics.NONE)
    },
    test("deleteItem metrics(Size) produces returnItemCollectionMetrics SIZE in the request") {
      var captured: DeleteItemRequest = null
      val interp                      =
        new DummyIOInterpreter(stub(onDeleteItem = req => { captured = req; DeleteItemResponse.builder().build() }))
      interp.run(DynamoDBQuery.deleteItem(table, pk).metrics(ReturnItemCollectionMetrics.Size)).unsafeRun()
      assertTrue(captured.returnItemCollectionMetrics() == AwsItemMetrics.SIZE)
    },
    test("batchWriteItem metrics(Size) produces returnItemCollectionMetrics SIZE in the request") {
      var captured: BatchWriteItemRequest = null
      val interp                          = new DummyIOInterpreter(stub(onBatchWrite = req => {
        captured = req; BatchWriteItemResponse.builder().build()
      }))
      interp
        .run(
          DynamoDBQuery
            .batchWriteItem(List(item))(i => DynamoDBQuery.putItem(table, i))
            .metrics(ReturnItemCollectionMetrics.Size)
        )
        .unsafeRun()
      assertTrue(captured.returnItemCollectionMetrics() == AwsItemMetrics.SIZE)
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 6: startKey flows through interpreter to AWS request
  // ---------------------------------------------------------------------------

  private val awsKey: java.util.Map[String, AwsAttrValue] =
    Map("id" -> AwsAttrValue.builder().s("a").build()).asJava

  private val startKeyRequestSuite = suite("startKey flows through interpreter to AWS request")(
    test("scan startKey(Some(pk)) sets exclusiveStartKey in the ScanRequest") {
      var captured: ScanRequest = null
      val interp                = new DummyIOInterpreter(stub(onScan = req => { captured = req; ScanResponse.builder().build() }))
      interp.run(DynamoDBQuery.scan(table, 10).startKey(Some(pk))).unsafeRun()
      assertTrue(captured.hasExclusiveStartKey() && captured.exclusiveStartKey() == awsKey)
    },
    test("scan with no startKey does not set exclusiveStartKey in the ScanRequest") {
      var captured: ScanRequest = null
      val interp                = new DummyIOInterpreter(stub(onScan = req => { captured = req; ScanResponse.builder().build() }))
      interp.run(DynamoDBQuery.scan(table, 10)).unsafeRun()
      assertTrue(!captured.hasExclusiveStartKey())
    },
    test("query startKey(Some(pk)) sets exclusiveStartKey in the QueryRequest") {
      var captured: QueryRequest = null
      val interp                 = new DummyIOInterpreter(stub(onQuery = req => { captured = req; QueryResponse.builder().build() }))
      interp.run(DynamoDBQuery.query(table, 10).startKey(Some(pk))).unsafeRun()
      assertTrue(captured.hasExclusiveStartKey() && captured.exclusiveStartKey() == awsKey)
    },
    test("query with no startKey does not set exclusiveStartKey in the QueryRequest") {
      var captured: QueryRequest = null
      val interp                 = new DummyIOInterpreter(stub(onQuery = req => { captured = req; QueryResponse.builder().build() }))
      interp.run(DynamoDBQuery.query(table, 10)).unsafeRun()
      assertTrue(!captured.hasExclusiveStartKey())
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 7: GSI / LSI flow through interpreter to AWS CreateTableRequest
  // ---------------------------------------------------------------------------

  private val createTableAttrs =
    NonEmptySet(
      AttributeDefinition.attrDefnString("id"),
      AttributeDefinition.attrDefnString("score")
    )

  private val createTableCompositeAttrs =
    NonEmptySet(
      AttributeDefinition.attrDefnString("id"),
      AttributeDefinition.attrDefnString("year"),
      AttributeDefinition.attrDefnString("score")
    )

  private val indexRequestSuite = suite("gsi/lsi flows through interpreter to CreateTableRequest")(
    test("gsi with throughput sets globalSecondaryIndexes in the request") {
      var captured: CreateTableRequest = null
      val interp                       = new DummyIOInterpreter(stub(onCreateTable = req => {
        captured = req; CreateTableResponse.builder().build()
      }))
      interp
        .run(
          DynamoDBQuery
            .createTable(table, KeySchema("id"), createTableAttrs, BillingMode.PayPerRequest)
            .gsi("score-index", KeySchema("score"), ProjectionType.All, 5L, 5L)
        )
        .unsafeRun()
      assertTrue(
        captured.hasGlobalSecondaryIndexes() &&
          captured.globalSecondaryIndexes().size() == 1 &&
          captured.globalSecondaryIndexes().get(0).indexName() == "score-index" &&
          captured.globalSecondaryIndexes().get(0).provisionedThroughput() != null &&
          captured.globalSecondaryIndexes().get(0).provisionedThroughput().readCapacityUnits() == 5L
      )
    },
    test("gsi without throughput sets globalSecondaryIndexes with no provisionedThroughput") {
      var captured: CreateTableRequest = null
      val interp                       = new DummyIOInterpreter(stub(onCreateTable = req => {
        captured = req; CreateTableResponse.builder().build()
      }))
      interp
        .run(
          DynamoDBQuery
            .createTable(table, KeySchema("id"), createTableAttrs, BillingMode.PayPerRequest)
            .gsi("score-index", KeySchema("score"), ProjectionType.KeysOnly)
        )
        .unsafeRun()
      assertTrue(
        captured.hasGlobalSecondaryIndexes() &&
          captured.globalSecondaryIndexes().get(0).indexName() == "score-index" &&
          captured.globalSecondaryIndexes().get(0).provisionedThroughput() == null
      )
    },
    test("lsi sets localSecondaryIndexes in the request") {
      var captured: CreateTableRequest = null
      val interp                       = new DummyIOInterpreter(stub(onCreateTable = req => {
        captured = req; CreateTableResponse.builder().build()
      }))
      interp
        .run(
          DynamoDBQuery
            .createTable(table, KeySchema("id", "year"), createTableCompositeAttrs, BillingMode.PayPerRequest)
            .lsi("score-index", KeySchema("id", "score"), ProjectionType.All)
        )
        .unsafeRun()
      assertTrue(
        captured.hasLocalSecondaryIndexes() &&
          captured.localSecondaryIndexes().size() == 1 &&
          captured.localSecondaryIndexes().get(0).indexName() == "score-index"
      )
    },
    test("no gsi/lsi means no index lists in the request") {
      var captured: CreateTableRequest = null
      val interp                       = new DummyIOInterpreter(stub(onCreateTable = req => {
        captured = req; CreateTableResponse.builder().build()
      }))
      interp
        .run(
          DynamoDBQuery.createTable(
            table,
            KeySchema("id"),
            NonEmptySet(AttributeDefinition.attrDefnString("id")),
            BillingMode.PayPerRequest
          )
        )
        .unsafeRun()
      assertTrue(!captured.hasGlobalSecondaryIndexes() && !captured.hasLocalSecondaryIndexes())
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 8: select flows through interpreter to AWS scan/query request
  // ---------------------------------------------------------------------------

  private val selectRequestSuite = suite("select flows through interpreter to AWS request")(
    test("scan.selectCount sets Select.COUNT in the ScanRequest") {
      var captured: ScanRequest = null
      val interp                = new DummyIOInterpreter(stub(onScan = req => { captured = req; ScanResponse.builder().build() }))
      interp.run(DynamoDBQuery.scan(table, 10).selectCount).unsafeRun()
      assertTrue(captured.select() == AwsSelect.COUNT)
    },
    test("scan.selectAllAttributes sets Select.ALL_ATTRIBUTES in the ScanRequest") {
      var captured: ScanRequest = null
      val interp                = new DummyIOInterpreter(stub(onScan = req => { captured = req; ScanResponse.builder().build() }))
      interp.run(DynamoDBQuery.scan(table, 10).selectAllAttributes).unsafeRun()
      assertTrue(captured.select() == AwsSelect.ALL_ATTRIBUTES)
    },
    test("scan with no select does not set Select in the ScanRequest") {
      var captured: ScanRequest = null
      val interp                = new DummyIOInterpreter(stub(onScan = req => { captured = req; ScanResponse.builder().build() }))
      interp.run(DynamoDBQuery.scan(table, 10)).unsafeRun()
      assertTrue(captured.select() == null)
    },
    test("query.selectCount sets Select.COUNT in the QueryRequest") {
      var captured: QueryRequest = null
      val interp                 = new DummyIOInterpreter(stub(onQuery = req => { captured = req; QueryResponse.builder().build() }))
      interp.run(DynamoDBQuery.query(table, 10).selectCount).unsafeRun()
      assertTrue(captured.select() == AwsSelect.COUNT)
    },
    test("query.selectAllProjectedAttributes sets Select.ALL_PROJECTED_ATTRIBUTES in the QueryRequest") {
      var captured: QueryRequest = null
      val interp                 = new DummyIOInterpreter(stub(onQuery = req => { captured = req; QueryResponse.builder().build() }))
      interp.run(DynamoDBQuery.query(table, 10).selectAllProjectedAttributes).unsafeRun()
      assertTrue(captured.select() == AwsSelect.ALL_PROJECTED_ATTRIBUTES)
    },
    test("query with no select does not set Select in the QueryRequest") {
      var captured: QueryRequest = null
      val interp                 = new DummyIOInterpreter(stub(onQuery = req => { captured = req; QueryResponse.builder().build() }))
      interp.run(DynamoDBQuery.query(table, 10)).unsafeRun()
      assertTrue(captured.select() == null)
    }
  )

  def spec = suite("AwsRequestFlow")(
    capacityRequestSuite,
    capacityInterceptorSuite,
    consistencyRequestSuite,
    returnsRequestSuite,
    metricsRequestSuite,
    startKeyRequestSuite,
    indexRequestSuite,
    selectRequestSuite
  )
}
