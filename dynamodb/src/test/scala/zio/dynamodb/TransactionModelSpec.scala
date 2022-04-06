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
  private val tableName                          = TableName("table")
  private val item                               = Item("a" -> 1)
  private val simpleGetItem                      = GetItem(tableName, item)
  private val emptyDynamoDB: ULayer[DynamoDb]    = DynamoDbMock.empty
  private val dynamoDB: ULayer[DynamoDb]         = DynamoDbMock.TransactGetItems(
    equalTo(DynamoDBExecutorImpl.constructGetTransaction(Chunk(simpleGetItem))),
    value(
      TransactGetItemsResponse(
        consumedCapacity = None,
        responses = Some(List(ItemResponse(Some(DynamoDBExecutorImpl.awsAttributeValueMap(item.map)))))
      ).asReadOnly
    )
  )
  private val clockLayer                         = ZLayer.identity[Has[Clock.Service]]
  override def spec: ZSpec[Environment, Failure] =
    suite("Transaction builder suite")(
      failureSuite.provideCustomLayer((emptyDynamoDB ++ clockLayer) >>> DynamoDBExecutor.live),
      successfulSuite
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
        val deleteTable = DeleteTable(tableName)

        assertM(deleteTable.transaction.execute)(equalTo(()))
      } @@ failing,
      testM("scan all") {
        val scanAll = ScanAll(tableName)
        assertM(scanAll.transaction.execute)(equalTo(zio.stream.Stream.empty))
      } @@ failing,
      testM("scan some") {
        val scanSome = ScanSome(tableName, 4)
        assertM(scanSome.transaction.execute)(equalTo((Chunk.empty, None)))
      } @@ failing,
      testM("describe table") {
        val describeTable = DescribeTable(tableName)
        assertM(describeTable.transaction.execute)(equalTo(DescribeTableResponse("", TableStatus.Creating)))
      } @@ failing,
      testM("query some") {
        val querySome = QuerySome(tableName, 4)
        assertM(querySome.transaction.execute)(equalTo((Chunk.empty, None)))
      } @@ failing,
      testM("query all") {
        val queryAll = QueryAll(tableName)
        assertM(queryAll.transaction.execute)(equalTo(zio.stream.Stream.empty))
      } @@ failing
    )
  )

  val successfulSuite = suite("transaction construction successes")(
    suite("transact get items")(
      testM("get item") {
        assertM(simpleGetItem.transaction.execute)(equalTo(Some(item)))
      }.provideCustomLayer((dynamoDB ++ clockLayer) >>> DynamoDBExecutor.live)
//      testM("batch get item") {
//        ???
//      }
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
