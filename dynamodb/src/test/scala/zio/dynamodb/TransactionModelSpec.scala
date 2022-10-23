package zio.dynamodb

import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.DynamoDb.DynamoDbMock
import zio.{ Chunk, Has, ULayer, ZLayer }
import zio.clock.Clock
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion.{ contains, equalTo, fails, hasField, isSubtype }
import zio.test.mock.Expectation.value
import zio.test.{ assertM, Assertion, DefaultRunnableSpec, ZSpec }
import io.github.vigoo.zioaws.dynamodb.model.{ ItemResponse, TransactGetItemsResponse, TransactWriteItemsResponse }

object TransactionModelSpec extends DefaultRunnableSpec {
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
  private val clockLayer                         = ZLayer.identity[Has[Clock.Service]]

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

  override def spec: ZSpec[Environment, Failure] =
    suite("Transaction builder suite")(
      failureSuite.provideCustomLayer((emptyDynamoDB ++ clockLayer) >>> DynamoDBExecutor.live),
      successfulSuite.provideCustomLayer((successCaseLayer ++ clockLayer) >>> DynamoDBExecutor.live)
    )

  val failureSuite = suite("transaction construction failures")(
    suite("mixed transaction types")(
      testM("mixing update and get") {
        val updateItem = UpdateItem(
          key = item,
          tableName = tableName,
          updateExpression = UpdateExpression($("name").set(""))
        )

        val getItem = GetItem(tableName, item)

        assertM(updateItem.zip(getItem).transaction.execute.run)(
          fails(isSubtype[MixedTransactionTypes](Assertion.anything))
        )
      }
    ),
    suite("invalid transaction actions")(
      testM("create table") {
        val createTable = CreateTable(
          tableName = tableName,
          keySchema = KeySchema("key"),
          attributeDefinitions = NonEmptySet(AttributeDefinition.attrDefnString("name")),
          billingMode = BillingMode.PayPerRequest
        )
        assertM(createTable.transaction.execute.run)(fails(invalidTransactionActionsContains(createTable)))
      },
      testM("delete table") {
        val deleteTable = DeleteTable(tableName)
        assertM(deleteTable.transaction.execute.run)(fails(invalidTransactionActionsContains(deleteTable)))
      },
      testM("scan all") {
        val scanAll = ScanAll(tableName)
        assertM(scanAll.transaction.execute.run)(fails(invalidTransactionActionsContains(scanAll)))
      },
      testM("scan some") {
        val scanSome = ScanSome(tableName, 4)
        assertM(scanSome.transaction.execute.run)(fails(invalidTransactionActionsContains(scanSome)))
      },
      testM("describe table") {
        val describeTable = DescribeTable(tableName)
        assertM(describeTable.transaction.execute.run)(fails(invalidTransactionActionsContains(describeTable)))
      },
      testM("query some") {
        val querySome = QuerySome(tableName, 4)
        assertM(querySome.transaction.execute.run)(fails(invalidTransactionActionsContains(querySome)))
      },
      testM("query all") {
        val queryAll = QueryAll(tableName)
        assertM(queryAll.transaction.execute.run)(fails(invalidTransactionActionsContains(queryAll)))
      }
    )
  )

  val successfulSuite = suite("transaction construction successes")(
    suite("transact get items")(
      testM("get item") {
        assertM(simpleGetItem.transaction.execute)(equalTo(Some(item)))
      },
      testM("batch get item") {
        assertM(simpleBatchGet.transaction.execute)(
          equalTo(
            BatchGetItem.Response(responses =
              MapOfSet.empty[TableName, Item].addAll((tableName, item), (tableName, item2))
            )
          )
        )
      },
      testM("multi table batch get item") {
        assertM(multiTableGet.transaction.execute)(
          equalTo(
            BatchGetItem.Response(responses =
              MapOfSet.empty[TableName, Item].addAll((tableName, item), (tableName2, item3), (tableName, item2))
            )
          )
        )
      }
    ),
    suite("transact write items")(
      testM("update item") {
        assertM(simpleUpdateItem.transaction.execute)(equalTo(None))
      },
      testM("delete item") {
        assertM(simpleDeleteItem.transaction.execute)(equalTo(None))
      },
      testM("put item") {
        assertM(simplePutItem.transaction.execute)(equalTo(None))
      },
      testM("batch write item") {
        assertM(simpleBatchWrite.transaction.execute)(equalTo(BatchWriteItem.Response(None)))
      }
    )
  )

}
