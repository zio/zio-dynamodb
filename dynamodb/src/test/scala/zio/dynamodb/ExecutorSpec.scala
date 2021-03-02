package zio.dynamodb

import zio.ZIO
import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, PutItem }
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec }

import scala.Predef.{ println, Map => ScalaMap }

object ExecutorSpec extends DefaultRunnableSpec {
//  def target[A](in: A): ZIO[Any, Exception, A] =
//    in match {
//      case one   =>
//      case (one) =>
//    }

  val emptyItem: Item = Item(ScalaMap.empty)

  override def spec =
    suite(label = "Executor")(
      testM(label = "should") {
        val primaryKey                                              = PrimaryKey(ScalaMap.empty)
        val getItem1: GetItem                                       = GetItem(key = primaryKey, tableName = TableName("T1"))
        val getItem2                                                = GetItem(key = primaryKey, tableName = TableName("T2"))
        val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2

        val putItem                                   = PutItem(tableName = TableName("T1"), item = Item(ScalaMap.empty))
        val deleteItem                                = DeleteItem(tableName = TableName("T2"), key = primaryKey)
        val zippedWrites: DynamoDBQuery[(Unit, Unit)] = putItem zip deleteItem

        println(s"$zippedGets $zippedWrites")

        for {
          ddb  <- ZIO.service[DynamoDb.Service]
          rtrn <- ddb.executeQueryAgainstDdb(getItem1)
        } yield assert(rtrn)(equalTo(Some(emptyItem)))
      }.provideCustomLayer(DynamoDb.test(Some(emptyItem)))
    )
}
