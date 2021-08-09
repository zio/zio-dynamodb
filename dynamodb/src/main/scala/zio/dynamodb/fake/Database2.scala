package zio.dynamodb.fake

import zio.dynamodb.{ AttributeValue, Item, LastEvaluatedKey, PrimaryKey }
import zio.stm.{ STM, TMap, USTM }
import zio.stream.ZStream
import zio.{ Chunk, Has, IO, UIO, ULayer, ZIO, ZLayer }

private[fake] final case class Database2(
  map: Map[String, Map[PrimaryKey, Item]] = Map.empty,
  tablePkMap: Map[String, String] = Map.empty,
  dbMap: TMap[String, TMap[PrimaryKey, Item]] = null, // TODO: remove null defaults
  tablePkMap2: TMap[String, String] = null
) {
  self =>

  //  val tfoo: USTM[TMap[PrimaryKey, Item]] = TMap.empty[PrimaryKey, Item]
  //  for {
  //    x <- tfoo
  //  y <- x.put()
  //  } yield ()

  def table(tableName: String, pkFieldName: String)(entries: TableEntry*): Database2  =
    Database2(self.map + (tableName -> entries.toMap), self.tablePkMap + (tableName -> pkFieldName))

  def table2(tableName: String, pkFieldName: String)(entries: TableEntry*): UIO[Unit] =
    (for {
      _        <- self.tablePkMap2.put(tableName, pkFieldName)
      innerMap <- TMap.fromIterable[PrimaryKey, Item](entries)
      _        <- self.dbMap.put(tableName, innerMap)
    } yield ()).commit

  def getItem(tableName: String, pk: PrimaryKey): Either[DatabaseError, Option[Item]] =
    self.map.get(tableName).map(_.get(pk)).toRight(TableDoesNotExists(tableName))

  def getItem2(tableName: String, pk: PrimaryKey): IO[DatabaseError, Option[Item]] =
    (for {
      o         <- self.dbMap.get(tableName)
      maybeItem <- o.fold[STM[DatabaseError, Option[Item]]](STM.fail[DatabaseError](TableDoesNotExists(tableName)))(
                     tmap => tmap.get(pk)
                   )
    } yield maybeItem).commit

  def put(tableName: String, item: Item): Either[DatabaseError, Database2] =
    tablePkMap.get(tableName).toRight(TableDoesNotExists(tableName)).flatMap { pkName =>
      val pk    = Item(item.map.filter { case (key, _) => key == pkName })
      val entry = pk -> item
      self.map
        .get(tableName)
        .toRight(TableDoesNotExists(tableName))
        .map(m => Database2(self.map + (tableName -> (m + entry)), self.tablePkMap))
    }

//  def put2(tableName: String, item: Item): IO[DatabaseError, Unit] =
//    (for {
//      o         <- self.dbMap.get(tableName)
//      innerTMap <-
//        o.fold[STM[DatabaseError, TMap[PrimaryKey, Item]]](STM.fail[DatabaseError](TableDoesNotExists(tableName)))(
//          STM.succeed(_)
//        )
//      pk        <- self.tablePkMap2
//                     .get(tableName)
//                     .fold[STM[DatabaseError, PrimaryKey]](STM.fail[DatabaseError](TableDoesNotExists(tableName))) { pkName =>
//                       STM.succeed(pkName)
//                     }
//    } yield ())

  def delete(tableName: String, pk: PrimaryKey): Either[DatabaseError, Database2] =
    self.map
      .get(tableName)
      .toRight(TableDoesNotExists(tableName))
      .map(m => Database2(self.map + (tableName -> (m - pk)), self.tablePkMap))

  def scanSome(
    tableName: String,
    exclusiveStartKey: LastEvaluatedKey,
    limit: Int
  ): Either[DatabaseError, (Chunk[Item], LastEvaluatedKey)] = {
    val items = for {
      itemMap <- self.map.get(tableName).toRight(TableDoesNotExists(tableName))
      pkName  <- tablePkMap.get(tableName).toRight(TableDoesNotExists(tableName))
      xs      <- Right(slice(sort(itemMap.toList, pkName), exclusiveStartKey, limit))
    } yield xs
    items
  }

  def scanAll[R](tableName: String, limit: Int): ZStream[R, DatabaseError, Item] = {
    val start: LastEvaluatedKey = None
    ZStream
      .paginateM(start) { lek =>
        ZIO.fromEither(scanSome(tableName, lek, limit)).map {
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
    val x = for {
      dbMap      <- TMap.empty[String, TMap[PrimaryKey, Item]].commit
      tablePkMap <- TMap.empty[String, String].commit
    } yield Database2(dbMap = dbMap, tablePkMap2 = tablePkMap)

    x.toLayer
  }
}
