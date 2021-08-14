package zio.dynamodb.fake

import zio.dynamodb.DatabaseError.TableDoesNotExists
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.stm.{ STM, TMap, ZSTM }
import zio.stream.ZStream
import zio.{ Chunk, IO, UIO, ZIO }

private[fake] final case class FakeDynamoDBExecutorImpl private (
  tableMap: TMap[String, TMap[PrimaryKey, Item]],
  tablePkNameMap: TMap[String, String]
) extends DynamoDBExecutor.Service {
  self =>

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
    atomicQuery match {
      case BatchGetItem(requestItemsMap, _, _)                                    =>
        val requestItems: Seq[(TableName, Set[TableGet])] = requestItemsMap.toList

        val zioPairs: IO[DatabaseError, Seq[(TableName, Option[Item])]] =
          ZIO
            .foreach(requestItems) {
              case (tableName, setOfTableGet) =>
                ZIO.foreach(setOfTableGet) { tableGet =>
                  getItem(tableName.value, tableGet.key).map((tableName, _))
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

      case BatchWriteItem(requestItems, _, _, _)                                  =>
        val results: ZIO[Any, DatabaseError, Unit] = ZIO.foreach_(requestItems.toList) {
          case (tableName, setOfWrite) =>
            ZIO.foreach_(setOfWrite) { write =>
              write match {
                case BatchWriteItem.Put(item)  =>
                  put(tableName.value, item)
                case BatchWriteItem.Delete(pk) =>
                  delete(tableName.value, pk)
              }
            }

        }
        results

      case GetItem(tableName, key, _, _, _)                                       =>
        getItem(tableName.value, key)

      case PutItem(tableName, item, _, _, _, _)                                   =>
        put(tableName.value, item)

      // TODO Note UpdateItem is not currently supported as it uses an UpdateExpression
      case UpdateItem(_, _, _, _, _, _, _)                                        =>
        ZIO.succeed(())

      case DeleteItem(tableName, key, _, _, _, _)                                 =>
        delete(tableName.value, key)

      case ScanSome(tableName, _, limit, _, exclusiveStartKey, _, _, _, _)        =>
        scanSome(tableName.value, exclusiveStartKey, Some(limit))

      case ScanAll(tableName, _, maybeLimit, _, _, _, _, _, _)                    =>
        scanAll(tableName.value, maybeLimit)

      case QuerySome(tableName, _, limit, _, exclusiveStartKey, _, _, _, _, _, _) =>
        scanSome(tableName.value, exclusiveStartKey, Some(limit))

      case QueryAll(tableName, _, maybeLimit, _, _, _, _, _, _, _, _)             =>
        scanAll(tableName.value, maybeLimit)

      // TODO: implement CreateTable

      case unknown                                                                =>
        ZIO.fail(new Exception(s"Constructor $unknown not implemented yet"))
    }

  private def tableMapAndPkName(tableName: String): ZSTM[Any, DatabaseError, (TMap[PrimaryKey, Item], String)] =
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

  private def pkForItem(item: Item, pkName: String): PrimaryKey                           =
    Item(item.map.filter { case (key, _) => key == pkName })

  private def getItem(tableName: String, pk: PrimaryKey): IO[DatabaseError, Option[Item]] =
    (for {
      (tableMap, _) <- tableMapAndPkName(tableName)
      maybeItem     <- tableMap.get(pk)
    } yield maybeItem).commit

  private def put(tableName: String, item: Item): IO[DatabaseError, Unit] =
    (for {
      (tableMap, pkName) <- tableMapAndPkName(tableName)
      pk                  = pkForItem(item, pkName)
      result             <- tableMap.put(pk, item)
    } yield result).commit

  private def delete(tableName: String, pk: PrimaryKey): IO[DatabaseError, Unit] =
    (for {
      (tableMap, _) <- tableMapAndPkName(tableName)
      result        <- tableMap.delete(pk)
    } yield result).commit

  private def scanSome(
    tableName: String,
    exclusiveStartKey: LastEvaluatedKey,
    maybeLimit: Option[Int]
  ): IO[DatabaseError, (Chunk[Item], LastEvaluatedKey)] =
    (for {
      (itemMap, pkName) <- tableMapAndPkName(tableName)
      xs                <- itemMap.toList
      result            <- STM.succeed(slice(sort(xs, pkName), exclusiveStartKey, maybeLimit))
    } yield result).commit

  private def scanAll[R](tableName: String, maybeLimit: Option[Int]): UIO[ZStream[Any, DatabaseError, Item]] = {
    val start: LastEvaluatedKey = None
    ZIO.succeed(
      ZStream
        .paginateM(start) { lek =>
          scanSome(tableName, lek, maybeLimit).map {
            case (chunk, lek) =>
              lek match {
                case None => (chunk, None)
                case lek  => (chunk, Some(lek))
              }
          }
        }
        .flattenChunks
    )

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
    maybeLimit: Option[Int]
  ): (Chunk[Item], LastEvaluatedKey) =
    maybeNextIndex(xs, exclusiveStartKey).map { index =>
      val limit              = maybeLimit.getOrElse(xs.length)
      val slice              = xs.slice(index, index + limit)
      val chunk: Chunk[Item] = Chunk.fromIterable(slice.map { case (_, item) => item })
      val lek                =
        if (index + limit >= xs.length) None
        else {
          val (pk, _) = slice.last
          Some(pk)
        }
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

object FakeDynamoDBExecutorImpl {
  def make(tableInfos: List[TableSchemaAndData]): UIO[FakeDynamoDBExecutorImpl] =
    (for {
      tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
      tablePkNameMap <- TMap.empty[String, String]
      _              <- STM.foreach(tableInfos) { tableInfo =>
                          for {
                            _    <- tablePkNameMap.put(tableInfo.tableName, tableInfo.pkName)
                            tmap <- TMap.empty[PrimaryKey, Item]
                            _    <- STM.foreach(tableInfo.entries)(entry => tmap.put(entry._1, entry._2))
                            _    <- tableMap.put(tableInfo.tableName, tmap)
                          } yield ()
                        }
    } yield FakeDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
}
