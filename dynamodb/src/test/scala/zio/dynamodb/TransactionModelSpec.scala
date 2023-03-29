package zio.dynamodb

import com.github.ghik.silencer.silent
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.DynamoDbMock
import zio.{ Chunk, ULayer }
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion.{ contains, equalTo, fails, hasField, isSubtype }
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

  @silent("toIterable")
  private def invalidTransactionActionsContains(action: DynamoDBQuery[Any, Any]): Assertion[Any] =
    isSubtype[InvalidTransactionActions](
      hasField(
        "invalidActions",
        a => {
          val b: Iterable[DynamoDBQuery[Any, Any]] = a.invalidActions.toIterable
          b
        },
        contains(action)
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
          fails(isSubtype[MixedTransactionTypes](Assertion.anything))
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
        assertZIO(createTable.transaction.execute.exit)(fails(invalidTransactionActionsContains(createTable)))
      },
      test("delete table") {
        val deleteTable = DeleteTable(tableName)
        assertZIO(deleteTable.transaction.execute.exit)(fails(invalidTransactionActionsContains(deleteTable)))
      },
      test("scan all") {
        val scanAll = ScanAll(tableName)
        assertZIO(scanAll.transaction.execute.exit)(fails(invalidTransactionActionsContains(scanAll)))
      },
      test("scan some") {
        val scanSome = ScanSome(tableName, 4)
        assertZIO(scanSome.transaction.execute.exit)(fails(invalidTransactionActionsContains(scanSome)))
      },
      test("describe table") {
        val describeTable = DescribeTable(tableName)
        assertZIO(describeTable.transaction.execute.exit)(fails(invalidTransactionActionsContains(describeTable)))
      },
      test("query some") {
        val querySome = QuerySome(tableName, 4)
        assertZIO(querySome.transaction.execute.exit)(fails(invalidTransactionActionsContains(querySome)))
      },
      test("query all") {
        val queryAll = QueryAll(tableName)
        assertZIO(queryAll.transaction.execute.exit)(fails(invalidTransactionActionsContains(queryAll)))
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
