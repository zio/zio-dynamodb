package zio.dynamodb.fake

import zio.{ IO, Ref, UIO, ZIO }
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{
  BatchGetItem,
  BatchWriteItem,
  DeleteItem,
  GetItem,
  PutItem,
  QueryAll,
  QuerySome,
  ScanAll,
  ScanSome,
  UpdateItem
}
import zio.dynamodb.{ DynamoDBExecutor, DynamoDBQuery, Item, MapOfSet, TableName }

private[fake] class FakeDynamoDBExecutorImpl(dbRef: Ref[Database]) extends DynamoDBExecutor.Service {

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
    atomicQuery match {
      case BatchGetItem(requestItemsMap, _, _)                                                  =>
        println(s"BatchGetItem $requestItemsMap")
        val requestItems: Seq[(TableName, Set[TableGet])] = requestItemsMap.toList

        val zioPairs: IO[DatabaseError, Seq[(TableName, Option[Item])]] =
          ZIO
            .foreach(requestItems) {
              case (tableName, setOfTableGet) =>
                ZIO.foreach(setOfTableGet) { tableGet =>
                  dbRef.get.flatMap { db =>
                    ZIO.fromEither(db.getItem(tableName.value, tableGet.key)).map((tableName, _))
                  }
                }
            }
            .map(_.flatten)

        val response: IO[DatabaseError, BatchGetItem.Response] = for {
          pairs       <- zioPairs
          pairFiltered = pairs.collect { case (tableName, Some(item)) => (tableName, item) }
          mapOfSet     = pairFiltered.foldLeft[MapOfSet[TableName, Item]](MapOfSet.empty) {
                           case (acc, (tableName, listOfItems)) => acc + (tableName -> listOfItems)
                         }
        } yield BatchGetItem.Response(mapOfSet)

        response

      case BatchWriteItem(requestItems, capacity, metrics, addList)                             =>
        println(s"BatchWriteItem $requestItems $capacity $metrics $addList")
        val xs: Seq[(TableName, Set[BatchWriteItem.Write])] = requestItems.toList
        val results: ZIO[Any, DatabaseError, Unit]          = ZIO.foreach_(xs) {
          case (tableName, setOfWrite) =>
            ZIO.foreach_(setOfWrite) { write =>
              val value: UIO[Either[DatabaseError, Database]] = dbRef.modify { db =>
                write match {
                  case BatchWriteItem.Put(item)  =>
                    db.put(tableName.value, item)
                      .fold(
                        _ => (db.put(tableName.value, item), db),
                        updatedDB => (db.put(tableName.value, item), updatedDB)
                      )
                  case BatchWriteItem.Delete(pk) =>
                    db.delete(tableName.value, pk)
                      .fold(
                        _ => (db.delete(tableName.value, pk), db),
                        updatedDB => (db.delete(tableName.value, pk), updatedDB)
                      )
                }
              }
              value.absolve.unit
            }

        }
        // TODO: we could execute in a loop
        results

      case GetItem(tableName, key, projections, readConsistency, capacity)                      =>
        println(s"FakeDynamoDBExecutor GetItem $key $tableName $projections $readConsistency  $capacity")
        dbRef.get.flatMap(db => ZIO.fromEither(db.getItem(tableName.value, key)))

      case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
        println(
          s"FakeDynamoDBExecutor PutItem $tableName $item $conditionExpression $capacity $itemMetrics $returnValues"
        )
        dbRef.update(db => db.put(tableName.value, item).getOrElse(db)).unit

      // TODO Note UpdateItem is not currently supported as it uses an UpdateExpression
      case UpdateItem(_, _, _, _, _, _, _)                                                      =>
        ZIO.succeed(())

      case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
        println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
        dbRef.update(db => db.delete(tableName.value, key).getOrElse(db)).unit

      case ScanSome(tableName, _, limit, _, exclusiveStartKey, _, _, _, _)                      =>
        println(
          s"FakeDynamoDBExecutor $tableName, $limit, $exclusiveStartKey"
        )
        dbRef.get.flatMap(db => ZIO.fromEither(db.scanSome(tableName.value, exclusiveStartKey, limit)))

      case ScanAll(tableName, _, _, _, _, _, _, _)                                              =>
        println(
          s"$tableName"
        )
        val zioOfStream = dbRef.get.map(_.scanAll(tableName.value, limit = 100))
        zioOfStream

      case QuerySome(tableName, _, limit, _, exclusiveStartKey, _, _, _, _, _, _)               =>
        println(s"$tableName, $exclusiveStartKey,$limit")
        dbRef.get.flatMap(db => ZIO.fromEither(db.scanSome(tableName.value, exclusiveStartKey, limit)))

      case QueryAll(tableName, _, _, exclusiveStartKey, _, _, _, _, _, _)                       =>
        println(
          s"$tableName, $exclusiveStartKey"
        )
        val zioOfStream = dbRef.get.map(_.scanAll(tableName.value, limit = 100))
        zioOfStream

      // TODO: implement remaining constructors

      // TODO: remove
      case unknown                                                                              =>
        ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
    }

}
