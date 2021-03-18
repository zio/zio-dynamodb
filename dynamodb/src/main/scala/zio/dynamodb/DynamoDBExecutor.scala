package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, DeleteItem, GetItem, PutItem, UpdateItem }
import zio.{ Has, ZIO, ZLayer }

import scala.collection.immutable.{ Map => ScalaMap }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  // returns hard coded responses for now
  def test =
    ZLayer.succeed(new Service {
      override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
        val tableName1 = TableName("T1")
        val item1      = Item(ScalaMap("k1" -> AttributeValue.String("k1")))
        val item2      = Item(ScalaMap("k2" -> AttributeValue.String("k2")))

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
