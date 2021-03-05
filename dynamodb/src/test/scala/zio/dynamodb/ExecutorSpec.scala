package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.Aggregated
import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, PutItem }
import zio.test.{ assertCompletes, DefaultRunnableSpec }

import scala.collection.immutable.{ Map => ScalaMap }

object ExecutorSpec extends DefaultRunnableSpec {

  val emptyItem: Item = Item(ScalaMap.empty)

  override def spec =
    suite(label = "Executor")(
      test(label = "should aggregate stuff") {
        val primaryKey                                              = PrimaryKey(ScalaMap.empty)
        val getItem1: GetItem                                       = GetItem(key = primaryKey, tableName = TableName("T1"))
        val getItem2                                                = GetItem(key = primaryKey, tableName = TableName("T2"))
        val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2

        val putItem1 = PutItem(tableName = TableName("T1"), item = Item(ScalaMap.empty))
//        val putItem2 = PutItem(tableName = TableName("T2"), item = Item(ScalaMap.empty))

        val deleteItem1 = DeleteItem(tableName = TableName("T1"), primaryKey)

        println(s"$zippedGets")

        val batched2: Aggregated =
          DynamoDBExecutor.loop2(deleteItem1 zip putItem1 zip getItem2 zip getItem1, Aggregated())
//        val batched1 = DynamoDBExecutor.loop(getItem1 zip putItem, List.empty)
//        val xs       = DynamoDBExecutor.loop(getItem1 zip getItem2 zip putItem, List.empty)
//        val batched3 = DynamoDBExecutor.aggregate(xs)

        println(s"loop2=$batched2")
//        println(s"loop1=$batched1")
//        println(s"loop1.aggregated=$batched3")

        assertCompletes
      }.provideCustomLayer(DynamoDb.test(Some(emptyItem)))
    )
}
