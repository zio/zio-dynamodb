package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, GetItem, PutItem }
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.{ Ref, ULayer, ZIO }

object FakeDynamoDBExecutor {

  // TODO: use a Set[String] for external PK representation?
  def fake(map: Map[Item, Item] = Map.empty[Item, Item], pk: Item => PrimaryKey): ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(map)
    } yield new Fake(ref, pk)).toLayer

  class Fake(mapRef: Ref[Map[Item, Item]], pk: Item => PrimaryKey) extends DynamoDBExecutor.Service {
    println(pk)
    override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
      atomicQuery match {
        case BatchGetItem(requestItems, capacity, _)                                            =>
          println(s"$requestItems $capacity")
          val xs: Seq[(TableName, Set[TableGet])] = requestItems.toList

          val zioPairs: Seq[ZIO[Any, Nothing, (TableName, Option[Item])]] = xs.toList.map {
            case (tableName, setOfTableGet) =>
              val set: Set[ZIO[Any, Nothing, (TableName, Option[Item])]] =
                setOfTableGet.map(tableGet => mapRef.get.map(m => (tableName, m.get(tableGet.key))))
              set
          }.flatten

          val response: ZIO[Any, Nothing, BatchGetItem.Response] = for {
            pairs       <- ZIO.collectAll(zioPairs)
            pairFiltered = pairs.collect { case (tableName, Some(item)) => (tableName, item) }
            mapOfSet     = pairFiltered.foldLeft[MapOfSet[TableName, Item]](MapOfSet.empty) {
                             case (acc, (tableName, listOfItems)) => acc + (tableName -> listOfItems)
                           }
          } yield BatchGetItem.Response(mapOfSet)

          response

        // TODO: implement - dummy for now
        case BatchWriteItem(requestItems, capacity, metrics, addList)                           =>
          println(s"$requestItems $capacity $metrics $addList")
          // TODO: we could execute in a loop
          ZIO.succeed(())

        case GetItem(tableName, key, projections, readConsistency, capacity)                    =>
          println(s"$key $tableName $projections $readConsistency  $capacity")
          mapRef.get.map(_.get(key))

        case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues) =>
          println(s"$tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
          mapRef.update(map => map + (pk(item) -> item)).unit

        // TODO: remove
        case unknown                                                                            =>
          ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
      }
  }
}
