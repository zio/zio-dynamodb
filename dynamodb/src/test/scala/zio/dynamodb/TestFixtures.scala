package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, PutItem, Scan }
import zio.stream.ZStream
import scala.collection.immutable.{ Map => ScalaMap }

object TestFixtures {
  val emptyItem                         = Item(ScalaMap.empty)
  def someItem: Option[Item]            = Some(emptyItem)
  def item(a: String): Item             = Item(ScalaMap(a -> AttributeValue.String(a)))
  def someItem(a: String): Option[Item] = Some(item(a))

  val primaryKey                                              = PrimaryKey(ScalaMap.empty)
  val tableName1                                              = TableName("T1")
  val tableName2                                              = TableName("T2")
  val tableName3                                              = TableName("T3")
  val indexName1                                              = IndexName("I1")
  val getItem1                                                = GetItem(key = primaryKey, tableName = tableName1)
  val getItem2                                                = GetItem(key = primaryKey, tableName = tableName2)
  val getItem3                                                = GetItem(key = primaryKey, tableName = tableName3)
  val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2

  val putItem1    = PutItem(tableName = tableName1, item = Item(ScalaMap.empty))
  val deleteItem1 = DeleteItem(tableName = tableName2, key = PrimaryKey(ScalaMap.empty))
  val stream1     = ZStream(emptyItem)
  val scan1       = Scan(tableName1, indexName1)
}
