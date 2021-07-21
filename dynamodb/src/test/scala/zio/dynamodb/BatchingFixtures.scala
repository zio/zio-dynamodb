package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData.{ primaryKey1, primaryKey1_2, primaryKey2, primaryKey3 }
import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, PutItem }

//TODO: come up with more logical naming for row data get xxxxT1K1
//noinspection TypeAnnotation
trait BatchingFixtures {
  // TODO: I think TableName's can be just Strings now
  val tableName1    = TableName("T1")
  val tableName2    = TableName("T2")
  val tableName3    = TableName("T3")
  val indexName1    = IndexName("I1")
  val getItem1      = GetItem(key = primaryKey1, tableName = tableName1)
  val getItem1_2    = GetItem(key = primaryKey1_2, tableName = tableName1)
  val getItem2      = GetItem(key = primaryKey2, tableName = tableName1)
  val getItem3      = GetItem(key = primaryKey3, tableName = tableName3)
  val item1: Item   = getItem1.key
  val item1_2: Item = getItem1_2.key
  val item2: Item   = getItem2.key
  val item3: Item   = getItem3.key

  val putItem1    = PutItem(tableName = tableName1, item = Item("k1" -> "k1"))
  // TODO: replace this with putItem1_2
  val putItem2    = PutItem(tableName = tableName1, item = Item("k2" -> "k2"))
  val deleteItem1 = DeleteItem(tableName = tableName1, key = PrimaryKey.empty)
}
