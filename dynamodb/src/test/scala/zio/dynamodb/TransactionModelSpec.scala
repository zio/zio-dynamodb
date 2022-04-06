package zio.dynamodb

import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.DynamoDb.DynamoDbMock
import zio.{ Chunk, Has, ULayer, ZLayer }
import zio.clock.Clock
import zio.dynamodb.DynamoDBQuery.{
  CreateTable,
  DeleteTable,
  DescribeTable,
  DescribeTableResponse,
  GetItem,
  ScanAll,
  ScanSome,
  TableStatus,
  UpdateItem
}
import zio.dynamodb.ProjectionExpression.$
import zio.test.Assertion.equalTo
import zio.test.TestAspect.failing
import zio.test.{ assertM, DefaultRunnableSpec, ZSpec }

object TransactionModelSpec extends DefaultRunnableSpec {
  private val tableName                          = TableName("table")
  private val item                               = Item("a" -> 1)
  private val emptyDynamoDB: ULayer[DynamoDb]    = DynamoDbMock.empty
  private val clockLayer                         = ZLayer.identity[Has[Clock.Service]]
  override def spec: ZSpec[Environment, Failure] =
    suite("Transaction builder suite")(
      failureSuite.provideCustomLayer((emptyDynamoDB ++ clockLayer) >>> DynamoDBExecutor.live)
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
      } @@ failing
//      testM("query some") {
//        ???
//      },
//      testM("query all") {
//        ???
//      }
    )
  )

//  val successfulSuite = suite("transaction construction successes")(
//    suite("transact get items")(
//      testM("batch get item") {
//        ???
//      },
//      testM("get item") {
//        ???
//      }
//    ),
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
//  )

}
