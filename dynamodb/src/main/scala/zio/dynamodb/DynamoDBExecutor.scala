package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, DeleteItem, GetItem, PutItem, Scan, UpdateItem }
import zio.stream.ZStream
import zio.{ Has, ZIO, ZLayer }

import scala.collection.immutable.{ Map => ScalaMap }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  //noinspection TypeAnnotation
  object TestData {
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
    val getItem2    = GetItem(key = primaryKey2, tableName = tableName1)
    val getItem3    = GetItem(key = primaryKey3, tableName = tableName3)
    val item1       = Item(getItem1.key.value)
    val item2       = Item(getItem2.key.value)

    val putItem1    = PutItem(tableName = tableName1, item = Item(ScalaMap("k1" -> AttributeValue.String("k1"))))
    val putItem2    = PutItem(tableName = tableName1, item = Item(ScalaMap("k2" -> AttributeValue.String("k2"))))
    val updateItem1 = UpdateItem(tableName = tableName1, primaryKey1)
    val deleteItem1 = DeleteItem(tableName = tableName1, key = PrimaryKey(ScalaMap.empty))
    val stream1     = ZStream(emptyItem)
    val scan1       = Scan(tableName1, indexName1)
  }

  // returns hard coded responses for now
  def test = {
    import TestData._

    ZLayer.succeed(new Service {
      override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
        val tableName1 = TableName("T1")

        atomicQuery match {

          case BatchGetItem(requestItems, capacity, _)                                              =>
            println(s"$requestItems $capacity")
            // TODO: we could execute in a loop
            val responses = (MapOfSet.empty + (tableName1 -> item1)) + (tableName1 -> item2)
            ZIO.succeed(BatchGetItem.Response(responses, ScalaMap.empty))

          case BatchWriteItem(requestItems, capacity, metrics, addList)                             =>
            println(s"$requestItems $capacity $metrics $addList")
            // TODO: we could execute in a loop
            ZIO.succeed(BatchWriteItem.Response())

          case GetItem(key, tableName, readConsistency, projections, capacity)                      =>
            println(s"$key $tableName $readConsistency $projections $capacity")
            ZIO.succeed(Some(Item(ScalaMap.empty)))

          case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
            println(s"$tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case UpdateItem(_, _, _, _, _, _, _)                                                      =>
            ZIO.succeed(())

          case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
            println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case unknown                                                                              =>
            ZIO.fail(new Exception(s"$unknown not implemented yet"))
        }
      }

    })
  }
}
