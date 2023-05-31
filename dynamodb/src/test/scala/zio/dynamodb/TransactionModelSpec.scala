package zio.dynamodb

import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.DynamoDbMock
import zio.{ Chunk, ULayer, ZIO }
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion.{ equalTo, fails, hasField, hasSameElements, isSome, isSubtype }
import zio.test._
import zio.mock.Expectation.value
import zio.aws.dynamodb.model.{ ItemResponse, TransactGetItemsResponse, TransactWriteItemsResponse }

object TransactionModelSpec extends ZIOSpecDefault {
  private val tableName                       = TableName("table")
  private val tableName2                      = TableName("table2")
  private val item                            = Item("a" -> 1)
  private val item2                           = Item("a" -> 2)
  private val item3                           = Item("a" -> 3)
  private val simpleGetItem                   = GetItem(tableName, item)
  private val simpleGetItem2                  = GetItem(tableName, item2)
  private val simpleGetItem3                  = GetItem(tableName2, item3)
  private val simpleUpdateItem                = UpdateItem(tableName, item, UpdateExpression($("a").set(4)))
  private val simpleDeleteItem                = DeleteItem(tableName, item)
  private val simplePutItem                   = PutItem(tableName, item)
  private val simpleBatchWrite                = BatchWriteItem().addAll(simplePutItem, simpleDeleteItem)
  private val simpleBatchGet                  = BatchGetItem().addAll(simpleGetItem, simpleGetItem2)
  private val multiTableGet                   = BatchGetItem().addAll(simpleGetItem, simpleGetItem2, simpleGetItem3)
  private val emptyDynamoDB: ULayer[DynamoDb] = DynamoDbMock.empty
  val getTransaction                          = DynamoDbMock.TransactGetItems(
    equalTo(DynamoDBExecutorImpl.constructGetTransaction(Chunk(simpleGetItem), ReturnConsumedCapacity.None)),
    value(
      TransactGetItemsResponse(
        consumedCapacity = None,
        responses = Some(List(ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item.map)))))
      ).asReadOnly
    )
  )
  val batchGetTransaction                     = DynamoDbMock.TransactGetItems(
    equalTo(DynamoDBExecutorImpl.constructGetTransaction(Chunk(simpleBatchGet), ReturnConsumedCapacity.None)),
    value(
      TransactGetItemsResponse(
        consumedCapacity = None,
        responses = Some(
          List(
            ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item.map))),
            ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item2.map)))
          )
        )
      ).asReadOnly
    )
  )
  val multiTableBatchGet                      = DynamoDbMock.TransactGetItems(
    equalTo(DynamoDBExecutorImpl.constructGetTransaction(Chunk(multiTableGet), ReturnConsumedCapacity.None)),
    value(
      TransactGetItemsResponse(
        consumedCapacity = None,
        responses = Some(
          List(
            ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item.map))),
            ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item2.map))),
            ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item3.map)))
          )
        )
      ).asReadOnly
    )
  )
  val updateItem                              = DynamoDbMock.TransactWriteItems(
    equalTo(
      DynamoDBExecutorImpl.constructWriteTransaction(
        Chunk(simpleUpdateItem),
        None,
        ReturnConsumedCapacity.None,
        ReturnItemCollectionMetrics.None
      )
    ),
    value(TransactWriteItemsResponse().asReadOnly)
  )
  val deleteItem                              = DynamoDbMock.TransactWriteItems(
    equalTo(
      DynamoDBExecutorImpl.constructWriteTransaction(
        Chunk(simpleDeleteItem),
        None,
        ReturnConsumedCapacity.None,
        ReturnItemCollectionMetrics.None
      )
    ),
    value(TransactWriteItemsResponse().asReadOnly)
  )
  val putItem                                 = DynamoDbMock.TransactWriteItems(
    equalTo(
      DynamoDBExecutorImpl.constructWriteTransaction(
        Chunk(simplePutItem),
        None,
        ReturnConsumedCapacity.None,
        ReturnItemCollectionMetrics.None
      )
    ),
    value(TransactWriteItemsResponse().asReadOnly)
  )
  val batchWriteItem                          = DynamoDbMock.TransactWriteItems(
    equalTo(
      DynamoDBExecutorImpl.constructWriteTransaction(
        Chunk(simpleBatchWrite),
        None,
        ReturnConsumedCapacity.None,
        ReturnItemCollectionMetrics.None
      )
    ),
    value(TransactWriteItemsResponse().asReadOnly)
  )

  private val successCaseLayer: ULayer[DynamoDb] =
    multiTableBatchGet
      .or(batchGetTransaction)
      .or(getTransaction)
      .or(updateItem)
      .or(deleteItem)
      .or(putItem)
      .or(batchWriteItem)

  private def invalidTransactionActionContains(action: DynamoDBQuery[Any, Any]): Assertion[Any] =
    isSubtype[InvalidTransactionAction](
      hasField(
        "invalidAction",
        _.invalidAction,
        equalTo(action)
      )
    )

  private def mixedTransactionTypesError(): Assertion[Any] =
    isSubtype[ValidationTransactionErrors](
      hasField(
        "mixedTransactionTypes",
        _.mixedTransactionTypes,
        isSome(equalTo(MixedTransactionTypes()))
      )
    )

  private def invalidTransactionActionsContains(actions: Seq[DynamoDBQuery[Any, Any]]): Assertion[Any] =
    isSubtype[ValidationTransactionErrors](
      hasField(
        "invalidActions",
        _.invalidActions.toList.map(_.invalidAction),
        hasSameElements(actions)
      )
    )

  override def spec =
    suite("Transaction builder suite")(
      failureSuite.provideLayer(emptyDynamoDB >>> DynamoDBExecutor.live),
      successfulSuite.provideLayer(successCaseLayer >>> DynamoDBExecutor.live)
    )

  val failureSuite = suite("transaction construction failures")(
    suite("mixed transaction types")(
      test("mixing update and get") {
        val updateItem = UpdateItem(
          key = item,
          tableName = tableName,
          updateExpression = UpdateExpression($("name").set(""))
        )

        val getItem                                                       = GetItem(tableName, item)
        val query: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap])] = updateItem.zip(getItem)

        assertZIO(query.transaction.execute.exit)(
          fails(mixedTransactionTypesError())
        )
      }
    ),
    suite("filterMixedTransactions")(
      test("mixing update and get and invalid actions describe and scan") {
        val updateItem = UpdateItem(
          key = item,
          tableName = tableName,
          updateExpression = UpdateExpression($("name").set(""))
        )

        val getItem  = GetItem(tableName, item)
        val describe = DescribeTable(tableName)
        val scan     = ScanSome(tableName, 4)
        val query    = Chunk(updateItem, getItem, describe, scan)

        assertZIO(ZIO.fromEither(DynamoDBExecutorImpl.filterMixedTransactions(query)).exit)(
          fails(mixedTransactionTypesError()) && fails(invalidTransactionActionsContains(List(describe, scan)))
        )
      }
    ),
    suite("invalid transaction actions")(
      test("create table") {
        val createTable = CreateTable(
          tableName = tableName,
          keySchema = KeySchema("key"),
          attributeDefinitions = NonEmptySet(AttributeDefinition.attrDefnString("name")),
          billingMode = BillingMode.PayPerRequest
        )
        assertZIO(createTable.transaction.execute.exit)(fails(invalidTransactionActionContains(createTable)))
      },
      test("delete table") {
        val deleteTable = DeleteTable(tableName)
        assertZIO(deleteTable.transaction.execute.exit)(fails(invalidTransactionActionContains(deleteTable)))
      },
      test("scan all") {
        val scanAll = ScanAll(tableName)
        assertZIO(scanAll.transaction.execute.exit)(fails(invalidTransactionActionContains(scanAll)))
      },
      test("scan some") {
        val scanSome = ScanSome(tableName, 4)
        assertZIO(scanSome.transaction.execute.exit)(fails(invalidTransactionActionContains(scanSome)))
      },
      test("describe table") {
        val describeTable = DescribeTable(tableName)
        assertZIO(describeTable.transaction.execute.exit)(fails(invalidTransactionActionContains(describeTable)))
      },
      test("query some") {
        val querySome = QuerySome(tableName, 4)
        assertZIO(querySome.transaction.execute.exit)(fails(invalidTransactionActionContains(querySome)))
      },
      test("query all") {
        val queryAll = QueryAll(tableName)
        assertZIO(queryAll.transaction.execute.exit)(fails(invalidTransactionActionContains(queryAll)))
      }
    )
  )

  val successfulSuite = suite("transaction construction successes")(
    suite("transact get items")(
      test("get item") {
        assertZIO(simpleGetItem.transaction.execute)(equalTo(Some(item)))
      },
      test("batch get item") {
        assertZIO(simpleBatchGet.transaction.execute)(
          equalTo(
            BatchGetItem.Response(responses =
              MapOfSet.empty[TableName, Item].addAll((tableName, item), (tableName, item2))
            )
          )
        )
      },
      test("multi table batch get item") {
        assertZIO(multiTableGet.transaction.execute)(
          equalTo(
            BatchGetItem.Response(responses =
              MapOfSet.empty[TableName, Item].addAll((tableName, item), (tableName2, item3), (tableName, item2))
            )
          )
        )
      }
    ),
    suite("transact write items")(
      test("update item") {
        assertZIO(simpleUpdateItem.transaction.execute)(equalTo(None))
      },
      test("delete item") {
        assertZIO(simpleDeleteItem.transaction.execute)(equalTo(None))
      },
      test("put item") {
        assertZIO(simplePutItem.transaction.execute)(equalTo(None))
      },
      test("batch write item") {
        assertZIO(simpleBatchWrite.transaction.execute)(equalTo(BatchWriteItem.Response(None)))
      }
    )
  )

}
