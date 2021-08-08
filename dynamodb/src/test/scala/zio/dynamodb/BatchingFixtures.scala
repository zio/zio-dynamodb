package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData.{ primaryKey1, primaryKey1_2, primaryKey2, primaryKey3, primaryKey3_2 }
import zio.dynamodb.DynamoDBQuery.{ getItem, DeleteItem, GetItem, PutItem }

//noinspection TypeAnnotation
trait BatchingFixtures {
  val tableName1                        = TableName("T1")
  val tableName2                        = TableName("T2")
  val tableName3                        = TableName("T3")
  val indexName1                        = IndexName("I1")
  def item(a: String): Item             = Item(a -> a)
  def someItem(a: String): Option[Item] = Some(item(a))
  def primaryKey(s: String)             = PrimaryKey(s -> s)
  def createGetItem(i: Int)             = getItem("T1", primaryKey(s"k$i"))

  val getItem1 =
    GetItem(key = primaryKey1, tableName = tableName1) // TODO: replace "K2" with "V2" when in value position
  val getItem1_2         = GetItem(key = primaryKey1_2, tableName = tableName1)
  val getItem1_NotExists = GetItem(key = PrimaryKey("k1" -> "NOT_EXISTS"), tableName = tableName1)
  val getItem2           = GetItem(key = primaryKey2, tableName = tableName1) // TODO: delete
  val getItem3           = GetItem(key = primaryKey3, tableName = tableName3)
  val getItem3_2         = GetItem(key = primaryKey3_2, tableName = tableName3)
  val item1: Item        = getItem1.key
  val item1_2: Item      = getItem1_2.key
  val item2: Item        = getItem2.key
  val item3: Item        = getItem3.key
  val item3_2: Item      = getItem3_2.key

  val putItem1    = PutItem(tableName = tableName1, item = Item("k1" -> "k1"))
  val putItem1_2  = PutItem(tableName = tableName1, item = primaryKey1_2)
  val putItem3    = PutItem(tableName = tableName3, item = primaryKey3)
  val putItem3_2  = PutItem(tableName = tableName3, item = primaryKey3_2)
  val deleteItem1 = DeleteItem(tableName = tableName1, key = PrimaryKey.empty)
  val deleteItem3 = DeleteItem(tableName = tableName3, key = primaryKey3)

}
