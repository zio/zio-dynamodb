package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, GetItem, PutItem }
import zio.{ Ref, ULayer, ZIO }

class Database(
  map: Map[String, Map[PrimaryKey, Item]] = Map.empty,
  tablePkMap: Map[String, String] = Map.empty
)               { self =>
  def getItem(tableName: String, pk: PrimaryKey): Option[Item] =
    self.map.get(tableName).flatMap(_.get(pk))

  def table(tableName: String, pkFieldName: String)(entries: (PrimaryKey, Item)*): Database =
    new Database(self.map + (tableName -> entries.toMap), self.tablePkMap + (tableName -> pkFieldName))

  def put(tableName: String, entry: (PrimaryKey, Item)): Option[Database]                   =
    self.map.get(tableName).map(m => new Database(self.map + (tableName -> (m + entry)), self.tablePkMap))

  def remove(tableName: String, pk: PrimaryKey): Option[Database]                           =
    self.map.get(tableName).map(m => new Database(self.map + (tableName -> (m - pk)), self.tablePkMap))

  def pkMap: Map[String, String]                                                            = self.tablePkMap
}
object Database {
  def apply() = new Database()
}

object FakeDynamoDBExecutor {

  def apply(db: Database = Database()): ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(db)
    } yield new Fake(ref)).toLayer

  private[dynamodb] class Fake(mapRef: Ref[Database]) extends DynamoDBExecutor.Service { self =>
    override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
      atomicQuery match {
        case BatchGetItem(requestItems, _, _)                                                   =>
          println(s"BatchGetItem $requestItems")
          val xs: Seq[(TableName, Set[TableGet])] = requestItems.toList

          val zioPairs: Seq[ZIO[Any, Nothing, (TableName, Option[Item])]] = xs.toList.flatMap {
            case (tableName, setOfTableGet) =>
              val set: Set[ZIO[Any, Nothing, (TableName, Option[Item])]] =
                setOfTableGet.map(tableGet =>
                  mapRef.get.map(db => (tableName, db.getItem(tableName.value, tableGet.key)))
                )
              set
          }

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
          println(s"BatchWriteItem $requestItems $capacity $metrics $addList")
          // TODO: we could execute in a loop
          ZIO.succeed(())

        case GetItem(tableName, key, projections, readConsistency, capacity)                    =>
          println(s"GetItem $key $tableName $projections $readConsistency  $capacity")
          mapRef.get.map(_.getItem(tableName.value, key))

        case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues) =>
          println(s"PutItemX $tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
          mapRef.update(db => db.put(tableName.value, primaryKey(db.pkMap)(item) -> item).getOrElse(db)).unit

        // TODO: remove
        case unknown                                                                            =>
          ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
      }

    def primaryKey(pkMap: Map[String, String]): Item => PrimaryKey =
      item => Item(item.map.filter { case (key, _) => pkMap.isDefinedAt(key) })
  }
}
