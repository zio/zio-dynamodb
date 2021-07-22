package zio.dynamodb.fake

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.{ DynamoDBQuery, _ }
import zio.{ Ref, UIO, ULayer, ZIO }

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

          val zioPairs: Seq[ZIO[Any, Nothing, (TableName, Option[Item])]] = xs.toList.flatMap {
            case (tableName, setOfTableGet) =>
              setOfTableGet.map(tableGet => dbRef.get.map(db => (tableName, db.getItem(tableName.value, tableGet.key))))
          }

          val response: ZIO[Any, Nothing, BatchGetItem.Response] = for {
            pairs       <- ZIO.collectAll(zioPairs)
            pairFiltered = pairs.collect { case (tableName, Some(item)) => (tableName, item) }
            mapOfSet     = pairFiltered.foldLeft[MapOfSet[TableName, Item]](MapOfSet.empty) {
                             case (acc, (tableName, listOfItems)) => acc + (tableName -> listOfItems)
                           }
          } yield BatchGetItem.Response(mapOfSet)

          response

        case BatchWriteItem(requestItems, capacity, metrics, addList)                             =>
          println(s"BatchWriteItem $requestItems $capacity $metrics $addList")
          val xs: Seq[(TableName, Set[BatchWriteItem.Write])] = requestItems.toList
          val results: Seq[UIO[Unit]]                         = xs.flatMap {
            case (tableName, setOfWrite) =>
              setOfWrite.map { write =>
                dbRef.update { db =>
                  write match {
                    case BatchWriteItem.Put(item)  =>
                      val maybeDatabase = db.put(tableName.value, item)
                      maybeDatabase.getOrElse(db)
                    case BatchWriteItem.Delete(pk) =>
                      db.delete(tableName.value, pk).getOrElse(db)
                  }
                }
              }

          }
          // TODO: we could execute in a loop
          ZIO.collectAll_(results)

        case GetItem(tableName, key, projections, readConsistency, capacity)                      =>
          println(s"FakeDynamoDBExecutor GetItem $key $tableName $projections $readConsistency  $capacity")
          dbRef.get.map(_.getItem(tableName.value, key))

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
