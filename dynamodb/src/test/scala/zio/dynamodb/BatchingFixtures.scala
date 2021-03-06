package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData.{ primaryKey1, primaryKey2, primaryKey3 }
import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, PutItem }

//noinspection TypeAnnotation
trait BatchingFixtures {
  val tableName1  = TableName("T1")
  val tableName2  = TableName("T2")
  val tableName3  = TableName("T3")
  val indexName1  = IndexName("I1")
  val getItem1    = GetItem(key = primaryKey1, tableName = tableName1)
  val getItem2    = GetItem(key = primaryKey2, tableName = tableName1)
  val getItem3    = GetItem(key = primaryKey3, tableName = tableName3)
  val item1: Item = getItem1.key
  val item2: Item = getItem2.key

  val putItem1    = PutItem(tableName = tableName1, item = Item("k1" -> "k1"))
  val putItem2    = PutItem(tableName = tableName1, item = Item("k2" -> "k2"))
  val deleteItem1 = DeleteItem(tableName = tableName1, key = PrimaryKey.empty)
}
