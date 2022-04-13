package zio.dynamodb

import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.DynamoDb.DynamoDbMock
import zio.{ Chunk, Has, ULayer, ZLayer }
import zio.clock.Clock
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion.equalTo
import zio.test.TestAspect.failing
import zio.test.mock.Expectation.value
import zio.test.{ assertM, DefaultRunnableSpec, ZSpec }
import io.github.vigoo.zioaws.dynamodb.model.{ ItemResponse, TransactGetItemsResponse }

object TransactionModelSpec extends DefaultRunnableSpec {
  private val tableName                       = TableName("table")
  private val tableName2                      = TableName("table2")
  private val item                            = Item("a" -> 1)
  private val item2                           = Item("a" -> 2)
  private val item3                           = Item("a" -> 3)
  private val simpleGetItem                   = GetItem(tableName, item)
  private val simpleGetItem2                  = GetItem(tableName, item2)
  private val simpleGetItem3                  = GetItem(tableName2, item3)
  private val simpleBatchGet                  = BatchGetItem().addAll(simpleGetItem, simpleGetItem2)
  private val multiTableGet                   = BatchGetItem().addAll(simpleGetItem, simpleGetItem2, simpleGetItem3)
  private val emptyDynamoDB: ULayer[DynamoDb] = DynamoDbMock.empty
  val getTransaction                          = DynamoDbMock.TransactGetItems(
    equalTo(DynamoDBExecutorImpl.constructGetTransaction(Chunk(simpleGetItem))),
    value(
      TransactGetItemsResponse(
        consumedCapacity = None,
        responses = Some(List(ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item.map)))))
      ).asReadOnly
    )
  )
  val batchGetTransaction                     = DynamoDbMock.TransactGetItems(
    equalTo(DynamoDBExecutorImpl.constructGetTransaction(Chunk(simpleBatchGet))),
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
    equalTo(DynamoDBExecutorImpl.constructGetTransaction(Chunk(multiTableGet))),
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

  private val getLayer: ULayer[DynamoDb]         = multiTableBatchGet.or(batchGetTransaction).or(getTransaction)
  private val clockLayer                         = ZLayer.identity[Has[Clock.Service]]
//  private val partialExecutor                    = (ZLayer.identity[Has[DynamoDb.Service]] ++ clockLayer) >>> DynamoDBExecutor.live
  override def spec: ZSpec[Environment, Failure] =
    suite("Transaction builder suite")(
      failureSuite.provideCustomLayer((emptyDynamoDB ++ clockLayer) >>> DynamoDBExecutor.live),
      successfulSuite.provideCustomLayer((getLayer ++ clockLayer) >>> DynamoDBExecutor.live)
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

        assertM(updateItem.zip(getItem).transaction.execute)(equalTo((None, None)))
      } @@ failing
    ),
    suite("invalid transaction actions")(
      testM("create table") {
        val createTable = CreateTable(
          tableName = tableName,
          keySchema = KeySchema("key"),
          attributeDefinitions = NonEmptySet(AttributeDefinition.attrDefnString("name")),
          billingMode = BillingMode.PayPerRequest
        )

        assertM(createTable.transaction.execute)(equalTo(()))
      } @@ failing,
      testM("delete table") {
        assertM(DeleteTable(tableName).transaction.execute)(equalTo(()))
      } @@ failing,
      testM("scan all") {
        assertM(ScanAll(tableName).transaction.execute)(equalTo(zio.stream.Stream.empty))
      } @@ failing,
      testM("scan some") {
        assertM(ScanSome(tableName, 4).transaction.execute)(equalTo((Chunk.empty, None)))
      } @@ failing,
      testM("describe table") {
        assertM(DescribeTable(tableName).transaction.execute)(equalTo(DescribeTableResponse("", TableStatus.Creating)))
      } @@ failing,
      testM("query some") {
        assertM(QuerySome(tableName, 4).transaction.execute)(equalTo((Chunk.empty, None)))
      } @@ failing,
      testM("query all") {
        assertM(QueryAll(tableName).transaction.execute)(equalTo(zio.stream.Stream.empty))
      } @@ failing
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
    )
//    suite("transact write items")(
//      testM("update item") {
//        ???
//      },
//      testM("delete item") {
//        ???
//      },
//      testM("put item") {
//        ???
//      },
//      testM("batch write item") {
//        ???
//      }
//    )
  )

}
