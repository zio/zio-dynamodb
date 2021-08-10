package zio.dynamodb.fake

import zio.dynamodb.{ AttributeValue, Item, LastEvaluatedKey, PrimaryKey }
import zio.stm.{ STM, TMap, ZSTM }
import zio.stream.ZStream
import zio.{ Chunk, Has, IO, UIO, ULayer }

private[fake] final case class Database2(
  tableMap: TMap[String, TMap[PrimaryKey, Item]],
  tablePkNameMap: TMap[String, String]
) {
  self =>

  def tableMapAndPkName(tableName: String): ZSTM[Any, DatabaseError, (TMap[PrimaryKey, Item], String)] =
    for {
      tableMap <- for {
                    maybeTableMap <- self.tableMap.get(tableName)
                    tableMap      <- maybeTableMap.fold[STM[DatabaseError, TMap[PrimaryKey, Item]]](
                                       STM.fail[DatabaseError](TableDoesNotExists(tableName))
                                     )(
                                       STM.succeed(_)
                                     )
                  } yield tableMap
      pkName   <- self.tablePkNameMap
                    .get(tableName)
                    .flatMap(_.fold[STM[DatabaseError, String]](STM.fail(TableDoesNotExists(tableName)))(STM.succeed(_)))
    } yield (tableMap, pkName)

  def pkForItem(item: Item, pkName: String): PrimaryKey = Item(item.map.filter { case (key, _) => key == pkName })

  def table(tableName: String, pkFieldName: String)(entries: TableEntry*): UIO[Unit] =
    (for {
      _        <- self.tablePkNameMap.put(tableName, pkFieldName)
      innerMap <- TMap.fromIterable[PrimaryKey, Item](entries)
      _        <- self.tableMap.put(tableName, innerMap)
    } yield ()).commit

  def getItem(tableName: String, pk: PrimaryKey): IO[DatabaseError, Option[Item]] =
    (for {
      (tableMap, _) <- tableMapAndPkName(tableName)
      maybeItem     <- tableMap.get(pk)
    } yield maybeItem).commit

  def put(tableName: String, item: Item): IO[DatabaseError, Unit] =
    (for {
      (tableMap, pkName) <- tableMapAndPkName(tableName)
      pk                  = pkForItem(item, pkName)
      result             <- tableMap.put(pk, item)
    } yield result).commit

  def delete(tableName: String, pk: PrimaryKey): IO[DatabaseError, Unit] =
    (for {
      (tableMap, _) <- tableMapAndPkName(tableName)
      result        <- tableMap.delete(pk)
    } yield result).commit

  def scanSome(
    tableName: String,
    exclusiveStartKey: LastEvaluatedKey,
    limit: Int
  ): IO[DatabaseError, (Chunk[Item], LastEvaluatedKey)] =
    (for {
      (itemMap, pkName) <- tableMapAndPkName(tableName)
      xs                <- itemMap.toList
      result            <- STM.succeed(slice(sort(xs, pkName), exclusiveStartKey, limit))
    } yield result).commit

  def scanAll[R](tableName: String, limit: Int): ZStream[R, DatabaseError, Item] = {
    val start: LastEvaluatedKey = None
    ZStream
      .paginateM(start) { lek =>
        scanSome(tableName, lek, limit).map {
          case (chunk, lek) =>
            lek match {
              case None => (chunk, None)
              case lek  => (chunk, Some(lek))
            }
        }
      }
      .flattenChunks
  }

  private def sort(xs: Seq[TableEntry], pkName: String): Seq[TableEntry] =
    xs.toList.sortWith {
      case ((pkL, _), (pkR, _)) =>
        (pkL.map.get(pkName), pkR.map.get(pkName)) match {
          case (Some(left), Some(right)) => attributeValueOrdering(left, right)
          case _                         => false
        }
    }

  private def slice(
    xs: Seq[TableEntry],
    exclusiveStartKey: LastEvaluatedKey,
    limit: Int
  ): (Chunk[Item], LastEvaluatedKey) =
    maybeNextIndex(xs, exclusiveStartKey).map { index =>
      val slice              = xs.slice(index, index + limit)
      val chunk: Chunk[Item] = Chunk.fromIterable(slice.map(_._2))
      val lek                = if (index + limit >= xs.length) None else Some(slice.last._1)
      (chunk, lek)
    }.getOrElse((Chunk.empty, None))

  private def maybeNextIndex(xs: Seq[TableEntry], exclusiveStartKey: LastEvaluatedKey): Option[Int] =
    exclusiveStartKey.fold[Option[Int]](Some(0)) { pk =>
      val foundIndex      = xs.indexWhere { case (pk2, _) => pk2 == pk }
      val afterFoundIndex =
        if (foundIndex == -1) None
        else Some(math.min(foundIndex + 1, xs.length))
      afterFoundIndex
    }

  // TODO come up with a correct ordering scheme - for now orderings are only correct for scalar types
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

object Database2 {
  lazy val live: ULayer[Has[Database2]] = {
    val database = for {
      tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]].commit
      tablePkNameMap <- TMap.empty[String, String].commit
    } yield Database2(tableMap, tablePkNameMap)

    database.toLayer
  }
}
