package zio.dynamodb.fake

import zio.Chunk
import zio.dynamodb.{ AttributeValue, Item, LastEvaluatedKey, PrimaryKey }

case class Database(
  map: Map[String, Map[PrimaryKey, Item]] = Map.empty,
  tablePkMap: Map[String, String] = Map.empty
) { self =>

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
