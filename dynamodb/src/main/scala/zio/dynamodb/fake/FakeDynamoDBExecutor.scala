package zio.dynamodb.fake

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.{ DynamoDBQuery, _ }
import zio.{ IO, Ref, ULayer, ZIO }

object FakeDynamoDBExecutor {

  def apply(db: Database = Database()): ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(db)
    } yield new Fake(ref)).toLayer

  private[fake] class Fake(dbRef: Ref[Database]) extends DynamoDBExecutor.Service { self =>

    override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
      atomicQuery match {
        case BatchGetItem(requestItems, _, _)                                                     =>
          println(s"BatchGetItem $requestItems")
          val xs: Seq[(TableName, Set[TableGet])] = requestItems.toList

          val zioPairs: ZIO[Any, DatabaseError, Seq[(TableName, Option[Item])]] =
            ZIO
              .foreach(xs) {
                case (tableName, setOfTableGet) =>
                  ZIO.foreach(setOfTableGet) { tableGet =>
                    dbRef.get.flatMap { db =>
                      ZIO.fromEither(db.getItem2(tableName.value, tableGet.key)).map((tableName, _))
                    }
                  }
              }
              .map(_.flatten)

          val response: ZIO[Any, DatabaseError, BatchGetItem.Response] = for {
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
                val value: IO[Nothing, Either[DatabaseError, Database]] = dbRef.modify { db =>
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
          dbRef.get.flatMap(db => ZIO.fromEither(db.getItem2(tableName.value, key)))

        case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
          println(
            s"FakeDynamoDBExecutor PutItem $tableName $item $conditionExpression $capacity $itemMetrics $returnValues"
          )
          dbRef.update(db => db.put(tableName.value, item).getOrElse(db)).unit

        // TODO Note UpdateItem is not supported as it uses an UpdateExpression
        case UpdateItem(_, _, _, _, _, _, _)                                                      =>
          ZIO.succeed(())

        case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
          println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
          dbRef.update(db => db.delete(tableName.value, key).getOrElse(db)).unit

        case ScanSome(
              tableName,
              _,
              limit,
              _,
              exclusiveStartKey,
              _,
              _,
              _,
              _
            ) =>
          println(
            s"FakeDynamoDBExecutor $tableName, $limit, $exclusiveStartKey"
          )
          dbRef.get.map(_.scanSome(tableName.value, exclusiveStartKey, limit))

        // TODO: implement remaining constructors

        // TODO: remove
        case unknown                                                                              =>
          ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
      }

  }
}
