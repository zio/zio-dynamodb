package zio.dynamodb

import zio.dynamodb.AttributeDefinition.{ attrDefnNumber, attrDefnString }
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.stream.ZStream
import zio.{ Has, ZIO }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  //noinspection TypeAnnotation
  object TestData {
    def primaryKey(i: Int) = PrimaryKey(s"$i" -> s"$i")
    val primaryKey1        = PrimaryKey("k1" -> "k1")
    val primaryKey1_2      = PrimaryKey("k1" -> "k2")
    val primaryKey2        = PrimaryKey("k2" -> "k2")
    val primaryKey3        = PrimaryKey("k3" -> "k3")
    val primaryKey3_2      = PrimaryKey("k3" -> "k4")

    val getItem1    = getItem("T1", primaryKey1)
    val getItem2    = getItem("T1", primaryKey2)
    val getItem3    = getItem("T3", primaryKey3)
    val item1: Item = primaryKey1
    val item2: Item = primaryKey2
    val item3: Item = primaryKey3

    val putItem1     = putItem("T1", item = Item("k1" -> "k1"))
    val putItem2     = putItem("T1", item = Item("k2" -> "k2"))
    val updateItem1  =
      updateItem("T1", PrimaryKey("k1" -> "k1"))(
        $("top[1]").remove
      )
    val deleteItem1  = deleteItem("T1", key = PrimaryKey.empty)
    val stream1      = ZStream(Item.empty)
    val scanPage1    = scanSome("T1", "I1", limit = 10)
    val queryPage1   = querySome("T1", "I1", limit = 10)
    val scanAll1     = scanAll("T1", "I1")
    val queryAll1    = queryAll("T1", "I1")
    val createTable1 = createTable(
      "T1",
      KeySchema("hashKey", "sortKey"),
      BillingMode.provisioned(readCapacityUnit = 10, writeCapacityUnit = 10)
    )(attrDefnString("attr1"), attrDefnNumber("attr2"))
  }

}
