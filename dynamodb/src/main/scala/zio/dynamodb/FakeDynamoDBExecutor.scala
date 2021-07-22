package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBExecutor.TestData.emptyItem
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, DeleteItem, GetItem, PutItem, QuerySome, UpdateItem }
import zio.{ Chunk, Ref, UIO, ULayer, ZIO }

// TODO: move to own fake package
case class Database(
  map: Map[String, Map[PrimaryKey, Item]] = Map.empty,
  tablePkMap: Map[String, String] = Map.empty
)               { self =>
  import Database._

  def getItem(tableName: String, pk: PrimaryKey): Option[Item] =
    self.map.get(tableName).flatMap(_.get(pk))

  // TODO: have just one param list to prevent () in the empty table case
  def table(tableName: String, pkFieldName: String)(entries: TableEntry*): Database =
    Database(self.map + (tableName -> entries.toMap), self.tablePkMap + (tableName -> pkFieldName))

  // TODO: consider returning just Database
  def put(tableName: String, item: Item): Option[Database]                          =
    tablePkMap.get(tableName).flatMap { pkName =>
      val pk    = Item(item.map.filter { case (key, _) => key == pkName })
      val entry = pk -> item
      println(s"put2 entry=$entry")
      self.map.get(tableName).map(m => Database(self.map + (tableName -> (m + entry)), self.tablePkMap))
    }

  // TODO: consider returning just Database
  def remove(tableName: String, pk: PrimaryKey): Option[Database] =
    self.map.get(tableName).map(m => Database(self.map + (tableName -> (m - pk)), self.tablePkMap))

  def scanSome(tableName: String, exclusiveStartKey: LastEvaluatedKey, limit: Int): (Chunk[Item], LastEvaluatedKey) = {
    val items: (Chunk[Item], LastEvaluatedKey) = (for {
      itemMap <- self.map.get(tableName)
      pkName  <- tablePkMap.get(tableName)
      xs      <- Some(range(sort(itemMap.toList, pkName), exclusiveStartKey, limit))
    } yield xs).getOrElse((Chunk.empty, None))
    items
  }

  private def sort(xs: Seq[TableEntry], pkName: String): Seq[TableEntry] =
    xs.toList.sortWith {
      case ((pkL, _), (pkR, _)) =>
        (pkL.map.get(pkName), pkR.map.get(pkName)) match {
          case (Some(left), Some(right)) => attributeValueOrdering(left, right)
          case _                         => false
        }
    }

  private def range(
    xs: Seq[TableEntry],
    exclusiveStartKey: LastEvaluatedKey,
    limit: Int
  ): (Chunk[Item], LastEvaluatedKey) = {
    val index = nextIndex(xs, exclusiveStartKey)
    if (index > -1) {
      val subset         = xs.slice(index, index + limit)
      val x: Chunk[Item] = Chunk.fromIterable(subset.map(_._2))
      val lek            = if (index + limit >= xs.length) None else Some(subset.last._1)
      (x, lek)
    } else
      (Chunk.empty, None)
  }

  // TODO: make return type Option
  // returns -1 if is exclusiveStartKey is not found, else next index
  private def nextIndex(xs: Seq[TableEntry], exclusiveStartKey: LastEvaluatedKey): Int =
    exclusiveStartKey.fold(0) { pk =>
      val foundIndex      = xs.indexWhere { case (pk2, _) => pk2 == pk }
      val afterFoundIndex =
        if (foundIndex == -1) -1
        else math.min(foundIndex + 1, xs.length)
      afterFoundIndex
    }

  // TODO: I lost the will to live here. TODO come up with a correct ordering scheme
  private def attributeValueOrdering(left: AttributeValue, right: AttributeValue): Boolean =
    (left, right) match {
      case (AttributeValue.Binary(valueL), AttributeValue.Binary(valueR))       =>
        valueL.toString.compareTo(valueR.toString) < 0
      case (AttributeValue.Bool(valueL), AttributeValue.Bool(valueR))           =>
        valueL.compareTo(valueR) < 0
      case (AttributeValue.List(valueL), AttributeValue.List(valueR))           =>
        valueL.toString.compareTo(valueR.toString) < 0
      case (AttributeValue.Map(valueL), AttributeValue.Map(valueR))             =>
        valueL.toString.compareTo(valueR.toString) < 0
      case (AttributeValue.Number(valueL), AttributeValue.Number(valueR))       =>
        valueL.compareTo(valueR) < 0
      case (AttributeValue.Null, AttributeValue.Null)                           =>
        false
      case (AttributeValue.String(valueL), AttributeValue.String(valueR))       =>
        valueL.compareTo(valueR) < 0
      case (AttributeValue.StringSet(valueL), AttributeValue.StringSet(valueR)) =>
        valueL.toString.compareTo(valueR.toString) < 0
      case _                                                                    => false
    }
}
object Database {
  type TableEntry = (PrimaryKey, Item)
}

object FakeDynamoDBExecutor {

  def apply(db: Database = Database()): ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(db)
    } yield new Fake(ref)).toLayer

  private[dynamodb] class Fake(dbRef: Ref[Database]) extends DynamoDBExecutor.Service { self =>
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
                      db.remove(tableName.value, pk).getOrElse(db)
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
          dbRef.update(db => db.remove(tableName.value, key).getOrElse(db)).unit

        case QuerySome(
              tableName,
              indexName,
              _,
              exclusiveStartKey,
              _,
              _,
              limit,
              _,
              _,
              _,
              _
            ) =>
          println(
            s"$tableName, $indexName, $exclusiveStartKey, $limit"
          )
          ZIO.succeed((Chunk(emptyItem), None))

        // TODO: implement remaining constructors

        // TODO: remove
        case unknown                                                                              =>
          ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
      }

  }
}
