package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, DeleteItem, GetItem, PutItem }
import zio.{ Has, ZIO, ZLayer }
import scala.collection.immutable.{ Map => ScalaMap }

object DynamoDb {
  type DynamoDb = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  // returns hard coded responses for now
  def test =
    ZLayer.succeed(new Service {
      override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
        atomicQuery match {

          case BatchGetItem(requestItems, capacity)                                                 =>
            println(s"$requestItems $capacity")
            // TODO: we could execute in a loop
            ZIO.succeed(BatchGetItem.Response(MapOfSet.empty, ScalaMap.empty))

          case BatchWriteItem(requestItems, capacity, metrics)                                      =>
            println(s"$requestItems $capacity $metrics")
            // TODO: we could execute in a loop
            ZIO.succeed(BatchWriteItem.Response(ScalaMap.empty, null))

          case GetItem(key, tableName, readConsistency, projections, capacity)                      =>
            println(s"$key $tableName $readConsistency $projections $capacity")
            ZIO.succeed(Some(Item(ScalaMap.empty)))

          case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
            println(s"$tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
            println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
            ZIO.succeed(())

          case x                                                                                    =>
            ZIO.fail(new Exception(s"$x not implemented yet"))
        }

    })
}
