package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, PutItem, Scan, UpdateItem }
import zio.stream.ZStream

import scala.collection.immutable.{ Map => ScalaMap }

//noinspection TypeAnnotation
object TestFixtures {
  val emptyItem                         = Item(ScalaMap.empty)
  def someItem: Option[Item]            = Some(emptyItem)
  def item(a: String): Item             = Item(ScalaMap(a -> AttributeValue.String(a)))
  def someItem(a: String): Option[Item] = Some(item(a))

  val primaryKey1 = PrimaryKey(ScalaMap("k1" -> AttributeValue.String("k1")))
  val primaryKey2 = PrimaryKey(ScalaMap("k2" -> AttributeValue.String("k2")))
  val primaryKey3 = PrimaryKey(ScalaMap("k3" -> AttributeValue.String("k3")))
  val tableName1  = TableName("T1")
  val tableName2  = TableName("T2")
  val tableName3  = TableName("T3")
  val indexName1  = IndexName("I1")
  val getItem1    = GetItem(key = primaryKey1, tableName = tableName1)
  val getItem2    = GetItem(key = primaryKey2, tableName = tableName2)
  val getItem3    = GetItem(key = primaryKey3, tableName = tableName3)

  val putItem1    = PutItem(tableName = tableName1, item = Item(ScalaMap("k1" -> AttributeValue.String("k1"))))
  val putItem2    = PutItem(tableName = tableName1, item = Item(ScalaMap("k2" -> AttributeValue.String("k2"))))
  val updateItem1 = UpdateItem(tableName = tableName1, primaryKey1)
  val deleteItem1 = DeleteItem(tableName = tableName1, key = PrimaryKey(ScalaMap.empty))
  val stream1     = ZStream(emptyItem)
  val scan1       = Scan(tableName1, indexName1)
}
