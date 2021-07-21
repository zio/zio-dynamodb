package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, GetItem, PutItem }
import zio.{ Ref, UIO, ULayer, ZIO }

case class Database(
  map: Map[String, Map[PrimaryKey, Item]] = Map.empty,
  tablePkMap: Map[String, String] = Map.empty
) { self =>
  def getItem(tableName: String, pk: PrimaryKey): Option[Item] =
    self.map.get(tableName).flatMap(_.get(pk))

  // TODO: have just one param list to prevent () in the empty table case
  def table(tableName: String, pkFieldName: String)(entries: (PrimaryKey, Item)*): Database =
    Database(self.map + (tableName -> entries.toMap), self.tablePkMap + (tableName -> pkFieldName))

  // TODO: consider returning just Database
  def put(tableName: String, item: Item): Option[Database]                                  =
    tablePkMap.get(tableName).flatMap { pkName =>
      val pk    = Item(item.map.filter { case (key, _) => key == pkName })
      val entry = pk -> item
      println(s"put2 entry=$entry")
      self.map.get(tableName).map(m => Database(self.map + (tableName -> (m + entry)), self.tablePkMap))
    }

  // TODO: consider returning just Database
  def remove(tableName: String, pk: PrimaryKey): Option[Database] =
    self.map.get(tableName).map(m => Database(self.map + (tableName -> (m - pk)), self.tablePkMap))
}

object FakeDynamoDBExecutor {

  def apply(db: Database = Database()): ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(db)
    } yield new Fake(ref)).toLayer

  private[dynamodb] class Fake(dbRef: Ref[Database]) extends DynamoDBExecutor.Service { self =>
    override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
      atomicQuery match {
        case BatchGetItem(requestItems, _, _)                                                   =>
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

        case BatchWriteItem(requestItems, capacity, metrics, addList)                           =>
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
                      db.remove(tableName.value, pk).getOrElse(db)
                  }
                }
              }

          }
          // TODO: we could execute in a loop
          ZIO.collectAll_(results)

        case GetItem(tableName, key, projections, readConsistency, capacity)                    =>
          println(s"FakeDynamoDBExecutor GetItem $key $tableName $projections $readConsistency  $capacity")
          dbRef.get.map(_.getItem(tableName.value, key))

        case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues) =>
          println(
            s"FakeDynamoDBExecutor PutItem $tableName $item $conditionExpression $capacity $itemMetrics $returnValues"
          )
          dbRef.update(db => db.put(tableName.value, item).getOrElse(db)).unit

        // TODO: implement remaining constructors

        // TODO: remove
        case unknown                                                                            =>
          ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
      }

  }
}
