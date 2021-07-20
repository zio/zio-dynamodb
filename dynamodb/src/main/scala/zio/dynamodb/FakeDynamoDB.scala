package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, GetItem, PutItem }
import zio.{ Ref, ULayer, ZIO }

/*
TODO
- multi table
val ddb = Database()
              .table("table1", "k1")(primaryKey1 -> item1, primaryKey2 -> item2)
              .table("table2", "k2")(primaryKey1 -> item1, primaryKey2 -> item2)

test(...).provideLayer(FakeDynamoDBExecutor(ddb))
 */
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

}
object Database {
  def apply() = new Database()
}

object FakeDynamoDBExecutor {

  def apply(map: Map[String, Map[Item, Item]] = Map.empty[String, Map[Item, Item]])(
    pkFields: String*
  ): ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(map)
    } yield new Fake(ref, pkFields.toSet)).toLayer

  class Fake(mapRef: Ref[Map[String, Map[Item, Item]]], pkFields: Set[String]) extends DynamoDBExecutor.Service {
    override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
      def getItem(map: Map[String, Map[Item, Item]], tableName: TableName, pk: PrimaryKey) =
        map.get(tableName.value).flatMap(_.get(pk))

      atomicQuery match {
        case BatchGetItem(requestItems, _, _)                                                   =>
          println(s"BatchGetItem $requestItems")
          val xs: Seq[(TableName, Set[TableGet])] = requestItems.toList

          val zioPairs: Seq[ZIO[Any, Nothing, (TableName, Option[Item])]] = xs.toList.flatMap {
            case (tableName, setOfTableGet) =>
              val set: Set[ZIO[Any, Nothing, (TableName, Option[Item])]] =
                setOfTableGet.map(tableGet => mapRef.get.map(m => (tableName, getItem(m, tableName, tableGet.key))))
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
//          val xs: Seq[(TableName, Set[BatchWriteItem.Write])] = requestItems.toList
//          val tableName                                       = TableName("tableName")
//          val tableWrite = Put(Item("" -> ""))
//          val x                                               = mapRef.update(m => m + )

          // TODO: we could execute in a loop
          ZIO.succeed(())

        case GetItem(tableName, key, projections, readConsistency, capacity)                    =>
          println(s"GetItem $key $tableName $projections $readConsistency  $capacity")
          mapRef.get.map( /*_.get(key)*/ getItem(_, tableName, key))

        case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues) =>
          println(s"PutItemX $tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
          mapRef.update(map => map + (tableName.value -> Map(primaryKey(item) -> item))).unit

        // TODO: remove
        case unknown                                                                            =>
          ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
      }
    }

    def primaryKey: Item => PrimaryKey =
      item => Item(item.map.filter { case (key, _) => pkFields.contains(key) })
  }
}
