package zio.dynamodb.fake

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.{ DynamoDBQuery, _ }
import zio.{ IO, Ref, UIO, ULayer, ZIO }

object FakeDynamoDBExecutor {

  /**
   * A Fake implementation of `DynamoDBExecutor.Service` with the very modest aspiration of providing bare minimum
   * functionality to enable internal unit tests and to enable very basic end to end examples that can serve as documentation -
   * only basic CRUD functionality is supported.
   * As such many features are currently not supported or have restrictions.
   *  - Supported
   *    - Basic CRUD operations including batch operations ie `BatchGetItem` and `BatchWriteItem`
   *  - Limited support
   *    - Primary Keys - only one primary key can be specified and it can have only one attribute which is only checked for equality
   *  - Not supported
   *    - Create table, Delete table
   *    - Expressions - these include Condition Expressions, Projection Expressions, UpdateExpressions, Filter Expressions
   *
   * '''Usage''': The schema has to be predefined using the `Database` class which has a builder style `table` method to specify a table,
   * a single primary and a var arg list of primary key/item pairs:
   * {{{testM("getItem") {
   *   for {
   *     result  <- GetItem(key = primaryKey1, tableName = tableName1).execute
   *     expected = Some(item1)
   *   } yield assert(result)(equalTo(expected))
   * }.provideLayer(FakeDynamoDBExecutor(Database()
   *   .table("tableName1", pkFieldName = "k1")(primaryKey1 -> item1, primaryKey1_2 -> item1_2)
   *   .table("tableName3", pkFieldName = "k3")(primaryKey3 -> item3)))}}}
   * @param db
   */
  def apply(db: Database = Database()): ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(db)
    } yield new Fake(ref)).toLayer

  private[fake] class Fake(dbRef: Ref[Database]) extends DynamoDBExecutor.Service { self =>

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
