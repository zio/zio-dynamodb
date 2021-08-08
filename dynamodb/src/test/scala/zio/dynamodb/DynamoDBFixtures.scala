package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ getItem, DeleteItem, GetItem, PutItem }

//noinspection TypeAnnotation
trait DynamoDBFixtures {
  val tableName1                        = TableName("T1")
  val tableName2                        = TableName("T2")
  val tableName3                        = TableName("T3")
  def item(a: String): Item             = Item(a -> a)
  def someItem(a: String): Option[Item] = Some(item(a))
  def createGetItem(i: Int)             = getItem("T1", PrimaryKey(s"k$i" -> s"v$i"))

  val primaryKey1   = PrimaryKey("k1" -> "v1")
  val primaryKey1_2 = PrimaryKey("k1" -> "v2")
  val primaryKey2   = PrimaryKey("k2" -> "v2")
  val primaryKey3   = PrimaryKey("k3" -> "v3")
  val primaryKey3_2 = PrimaryKey("k3" -> "v4")

  val getItem1           =
    GetItem(key = primaryKey1, tableName = tableName1)
  val getItem1_2         = GetItem(key = primaryKey1_2, tableName = tableName1)
  val getItem1_NotExists = GetItem(key = PrimaryKey("k1" -> "NOT_EXISTS"), tableName = tableName1)
  val getItem2           = GetItem(key = primaryKey2, tableName = tableName1)
  val getItem3           = GetItem(key = primaryKey3, tableName = tableName3)
  val getItem3_2         = GetItem(key = primaryKey3_2, tableName = tableName3)
  val item1: Item        = getItem1.key
  val item1_2: Item      = getItem1_2.key
  val item2: Item        = getItem2.key
  val item3: Item        = getItem3.key
  val item3_2: Item      = getItem3_2.key

  val putItem1    = PutItem(tableName = tableName1, item = primaryKey1)
  val putItem1_2  = PutItem(tableName = tableName1, item = primaryKey1_2)
  val putItem3_2  = PutItem(tableName = tableName3, item = primaryKey3_2)
  val deleteItem1 = DeleteItem(tableName = tableName1, key = PrimaryKey.empty)
  val deleteItem3 = DeleteItem(tableName = tableName3, key = primaryKey3)

}
