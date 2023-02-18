package zio.dynamodb
import zio.aws.core.FieldIsNone
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.primitives.{
  BinaryAttributeValue,
  BooleanAttributeValue,
  ExpressionAttributeNameVariable,
  KMSMasterKeyId,
  NonKeyAttributeName,
  NullAttributeValue,
  NumberAttributeValue,
  PositiveIntegerObject,
  PositiveLongObject,
  SSEEnabled,
  ScanSegment,
  ScanTotalSegments,
  StringAttributeValue,
  TagKeyString,
  TagValueString,
  AttributeName => ZIOAwsAttributeName,
  ClientRequestToken => ZIOAwsClientRequestToken,
  ConditionExpression => ZIOAwsConditionExpression,
  ConsistentRead => ZIOAwsConsistentRead,
  ExpressionAttributeValueVariable => ZIOAwsExpressionAttributeValueVariable,
  IndexName => ZIOAwsIndexName,
  KeyExpression => ZIOAwsKeyExpression,
  KeySchemaAttributeName => ZIOAwsKeySchemaAttributeName,
  ProjectionExpression => ZIOAwsProjectionExpression,
  TableName => ZIOAwsTableName,
  UpdateExpression => ZIOAwsUpdateExpression
}
import zio.aws.dynamodb.model.{
  BatchGetItemRequest,
  BatchWriteItemRequest,
  CreateTableRequest,
  DeleteItemRequest,
  DeleteRequest,
  DeleteTableRequest,
  DescribeTableRequest,
  Get,
  GetItemRequest,
  KeySchemaElement,
  KeyType,
  KeysAndAttributes,
  PutItemRequest,
  PutRequest,
  QueryRequest,
  ReturnValue,
  ScalarAttributeType,
  ScanRequest,
  Tag,
  TransactGetItem,
  TransactGetItemsRequest,
  TransactWriteItem,
  TransactWriteItemsRequest,
  UpdateItemRequest,
  UpdateItemResponse,
  WriteRequest,
  primitives,
  AttributeDefinition => ZIOAwsAttributeDefinition,
  AttributeValue => ZIOAwsAttributeValue,
  BillingMode => ZIOAwsBillingMode,
  ConditionCheck => ZIOAwsConditionCheck,
  Delete => ZIOAwsDelete,
  GlobalSecondaryIndex => ZIOAwsGlobalSecondaryIndex,
  LocalSecondaryIndex => ZIOAwsLocalSecondaryIndex,
  Projection => ZIOAwsProjection,
  ProjectionType => ZIOAwsProjectionType,
  ProvisionedThroughput => ZIOAwsProvisionedThroughput,
  Put => ZIOAwsPut,
  ReturnConsumedCapacity => ZIOAwsReturnConsumedCapacity,
  ReturnItemCollectionMetrics => ZIOAwsReturnItemCollectionMetrics,
  SSESpecification => ZIOAwsSSESpecification,
  SSEType => ZIOAwsSSEType,
  Select => ZIOAwsSelect,
  TableStatus => ZIOAwsTableStatus,
  Update => ZIOAwsUpdate
}
import zio.dynamodb.ConsistencyMode.toBoolean
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.SSESpecification.SSEType
import zio.stream.{ Stream, ZStream }
import zio.{ Chunk, NonEmptyChunk, ZIO }

import scala.collection.immutable.{ Map => ScalaMap }

private[dynamodb] final case class DynamoDBExecutorImpl private (dynamoDb: DynamoDb) extends DynamoDBExecutor {
  import DynamoDBExecutorImpl._

  def executeMap[A, B](map: Map[A, B]): ZIO[Any, Throwable, B] =
    execute(map.query).map(map.mapper)

  def executeZip[A, B, C](zip: Zip[A, B, C]): ZIO[Any, Throwable, C] =
    execute(zip.left).zipWith(execute(zip.right))(zip.zippable.zip)

  def executeConstructor[A](constructor: Constructor[_, A]): ZIO[Any, Throwable, A] =
    constructor match {
      case c: GetItem        => executeGetItem(c)
      case c: PutItem        => executePutItem(c)
      case c: BatchGetItem   => executeBatchGetItem(c)
      case c: BatchWriteItem => executeBatchWriteItem(c)
      case _: ConditionCheck => ZIO.none
      case c: ScanAll        => executeScanAll(c)
      case c: ScanSome       => executeScanSome(c)
      case c: UpdateItem     => executeUpdateItem(c)
      case c: CreateTable    => executeCreateTable(c)
      case c: DeleteItem     => executeDeleteItem(c)
      case c: DeleteTable    => executeDeleteTable(c)
      case c: DescribeTable  => executeDescribeTable(c)
      case c: QuerySome      => executeQuerySome(c)
      case c: QueryAll       => executeQueryAll(c)
      case c: Transaction[_] => executeTransaction(c)
      case Succeed(c)        => ZIO.succeed(c())
    }

  override def execute[A](atomicQuery: DynamoDBQuery[_, A]): ZIO[Any, Throwable, A] =
    atomicQuery match {
      case constructor: Constructor[_, A] => executeConstructor(constructor)
      case zip @ Zip(_, _, _)             => executeZip(zip)
      case map @ Map(_, _)                => executeMap(map)
    }

  private def executeCreateTable(createTable: CreateTable): ZIO[Any, Throwable, Unit] =
    dynamoDb
      .createTable(awsCreateTableRequest(createTable))
      .mapError(_.toThrowable)
      .unit

  private def executeDeleteItem(deleteItem: DeleteItem): ZIO[Any, Throwable, Option[Item]] =
    dynamoDb
      .deleteItem(awsDeleteItemRequest(deleteItem))
      .mapBoth(_.toThrowable, _.attributes.toOption.map(dynamoDBItem(_)))

  private def executeDeleteTable(deleteTable: DeleteTable): ZIO[Any, Throwable, Unit] =
    dynamoDb
      .deleteTable(DeleteTableRequest(ZIOAwsTableName(deleteTable.tableName.value)))
      .mapError(_.toThrowable)
      .unit

  private def executePutItem(putItem: PutItem): ZIO[Any, Throwable, Option[Item]] =
    dynamoDb.putItem(awsPutItemRequest(putItem)).mapBoth(_.toThrowable, _.attributes.toOption.map(dynamoDBItem(_)))

  private def executeGetItem(getItem: GetItem): ZIO[Any, Throwable, Option[Item]] =
    dynamoDb
      .getItem(awsGetItemRequest(getItem))
      .mapBoth(_.toThrowable, _.item.map(dynamoDBItem))
      .map(_.toOption)

  private def executeUpdateItem(updateItem: UpdateItem): ZIO[Any, Throwable, Option[Item]] =
    dynamoDb.updateItem(awsUpdateItemRequest(updateItem)).mapBoth(_.toThrowable, optionalItem)

  private def executeDescribeTable(describeTable: DescribeTable): ZIO[Any, Throwable, DescribeTableResponse] =
    dynamoDb
      .describeTable(DescribeTableRequest(ZIOAwsTableName(describeTable.tableName.value)))
      .flatMap(s =>
        for {
          table  <- ZIO.fromOption(s.table.toOption).orElseFail(FieldIsNone("table"))
          arn    <- ZIO.fromOption(table.tableArn.toOption).orElseFail(FieldIsNone("tableArn"))
          status <- ZIO.fromOption(table.tableStatus.toOption).orElseFail(FieldIsNone("tableStatus"))
        } yield DescribeTableResponse(tableArn = arn, tableStatus = dynamoDBTableStatus(status))
      )
      .mapError(_.toThrowable)

  private def executeQuerySome(querySome: QuerySome): ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] =
    dynamoDb
      .queryPaginated(awsQueryRequest(querySome))
      .mapBoth(
        _.toThrowable,
        queryResponse =>
          (
            queryResponse.items.map(list => Chunk.fromIterable(list.map(dynamoDBItem))).getOrElse(Chunk.empty[Item]),
            queryResponse.lastEvaluatedKey.map(dynamoDBItem).toOption
          )
      )

  private def executeQueryAll(queryAll: QueryAll): ZIO[Any, Throwable, Stream[Throwable, Item]] =
    ZIO.succeed(
      dynamoDb
        .query(awsQueryRequest(queryAll))
        .mapBoth(
          awsErr => awsErr.toThrowable,
          item => dynamoDBItem(item)
        )
    )

  private def executeBatchWriteItem(batchWriteItem: BatchWriteItem): ZIO[Any, Throwable, BatchWriteItem.Response] =
    if (batchWriteItem.requestItems.isEmpty) ZIO.succeed(BatchWriteItem.Response(None))
    else
      for {
        ref         <- zio.Ref.make[MapOfSet[TableName, BatchWriteItem.Write]](batchWriteItem.requestItems)
        _           <- (for {
                           unprocessedItems           <- ref.get
                           response                   <- dynamoDb
                                                           .batchWriteItem(
                                                             awsBatchWriteItemRequest(batchWriteItem.copy(requestItems = unprocessedItems))
                                                           )
                                                           .mapError(_.toThrowable)
                           responseUnprocessedItemsOpt = response.unprocessedItems
                                                           .map(map =>
                                                             mapOfListToMapOfSet(map.map {
                                                               case (k, v) => (TableName(k.toString), v)
                                                             })(writeRequestToBatchWrite)
                                                           )
                                                           .toOption
                           _                          <- responseUnprocessedItemsOpt match {
                                                           case Some(responseUnprocessedItems) => ref.set(responseUnprocessedItems)
                                                           case None                           => ZIO.unit
                                                         }
                           _                          <- ZIO
                                                           .fail(BatchRetryError())
                                                           .when(responseUnprocessedItemsOpt.exists(_.nonEmpty))

                         } yield ())
                         .retry(batchWriteItem.retryPolicy.whileInput {
                           case BatchRetryError() => true
                           case _                 => false
                         })
                         .catchSome(catchBatchRetryError)
        unprocessed <- ref.get
      } yield BatchWriteItem.Response(unprocessed.toOption)

  private def executeTransaction[A](transaction: Transaction[A]): ZIO[Any, Throwable, A] =
    for {
      transactionActionsAndMappings        <- ZIO.from(buildTransaction(transaction))
      transactionActionsAndTransactionType <- ZIO.fromEither(filterMixedTransactions(transactionActionsAndMappings._1))
      getOrWriteTransaction                 = constructTransaction(
                                                transactionActionsAndTransactionType._1,
                                                transactionActionsAndTransactionType._2,
                                                transaction.clientRequestToken,
                                                transaction.itemMetrics,
                                                transaction.capacity
                                              )
      transactionZIO                        = getOrWriteTransaction match {
                                                case Left(transactGetItems)    =>
                                                  (for {
                                                    response <- dynamoDb.transactGetItems(transactGetItems)
                                                    items     = response.responses.map(_.map(item => item.item.map(dynamoDBItem).toOption))
                                                  } yield items.map(Chunk.fromIterable(_)).getOrElse(Chunk.empty[Item]))
                                                    .mapError(_.toThrowable)
                                                case Right(transactWriteItems) =>
                                                  dynamoDb
                                                    .transactWriteItems(transactWriteItems)
                                                    .mapBoth(
                                                      _.toThrowable,
                                                      _ => Chunk.fill(transactionActionsAndTransactionType._1.length)(None)
                                                    )
                                              }
      itemChunk                            <- transactionZIO
    } yield transactionActionsAndMappings._2(itemChunk)

  private def executeScanSome(scanSome: ScanSome): ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] =
    dynamoDb
      .scanPaginated(awsScanRequest(scanSome))
      .mapBoth(
        _.toThrowable,
        scanResponse =>
          (
            scanResponse.items.map(list => Chunk.fromIterable(list.map(dynamoDBItem))).getOrElse(Chunk.empty[Item]),
            scanResponse.lastEvaluatedKey.map(dynamoDBItem).toOption
          )
      )

  private def executeScanAll(scanAll: ScanAll): ZIO[Any, Throwable, Stream[Throwable, Item]] =
    if (scanAll.totalSegments > 1) {
      lazy val emptyStream: ZStream[Any, Throwable, Item] = ZStream()
      for {
        streams <- ZIO.foreachPar(Chunk.fromIterable(0 until scanAll.totalSegments)) { segment =>
                     ZIO.succeed(
                       dynamoDb
                         .scan(
                           awsScanRequest(
                             scanAll,
                             Some(ScanAll.Segment(segment, scanAll.totalSegments))
                           )
                         )
                         .mapBoth(_.toThrowable, dynamoDBItem)
                     )
                   }
      } yield streams.foldLeft(emptyStream) { case (acc, stream) => acc.merge(stream) }
    } else
      ZIO.succeed(
        dynamoDb
          .scan(awsScanRequest(scanAll, None))
          .mapBoth(
            awsError => awsError.toThrowable,
            item => dynamoDBItem(item)
          )
      )

  private def executeBatchGetItem(batchGetItem: BatchGetItem): ZIO[Any, Throwable, BatchGetItem.Response] =
    if (batchGetItem.requestItems.isEmpty)
      ZIO.succeed(BatchGetItem.Response())
    else
      for {
        unprocessedKeys <- zio.Ref.make[ScalaMap[TableName, TableGet]](batchGetItem.requestItems)
        collectedItems  <- zio.Ref.make[MapOfSet[TableName, Item]](MapOfSet.empty)
        _               <- (for {
                               unprocessed           <- unprocessedKeys.get
                               currentCollection     <- collectedItems.get
                               response              <- dynamoDb
                                                          .batchGetItem(awsBatchGetItemRequest(batchGetItem.copy(requestItems = unprocessed)))
                                                          .mapError(_.toThrowable)
                               responseUnprocessedOpt = response.unprocessedKeys
                                                          .map(_.map { case (k, v) => (TableName(k), keysAndAttrsToTableGet(v)) })
                                                          .toOption
                               _                     <- responseUnprocessedOpt match {
                                                          case Some(responseUnprocessed) => unprocessedKeys.set(responseUnprocessed)
                                                          case None                      => ZIO.unit
                                                        }
                               retrievedItemsOpt      = response.responses.toOption
                               _                     <- retrievedItemsOpt match {
                                                          case Some(retrievedItems) =>
                                                            collectedItems.set(currentCollection ++ tableItemsMapToResponse(retrievedItems.map {
                                                              case (k, v) => (TableName(k.toString), v)
                                                            }))
                                                          case None                 => collectedItems.set(currentCollection)
                                                        }
                               _                     <- ZIO.fail(BatchRetryError()).when(responseUnprocessedOpt.exists(_.nonEmpty))
                             } yield ())
                             .retry(batchGetItem.retryPolicy.whileInput {
                               case BatchRetryError() => true
                               case _                 => false
                             })
                             .catchSome(catchBatchRetryError)

        retrievedItems  <- collectedItems.get
        unprocessed     <- unprocessedKeys.get
      } yield BatchGetItem.Response(responses = retrievedItems, unprocessedKeys = unprocessed)

}

case object DynamoDBExecutorImpl {

  sealed trait TransactionType
  object TransactionType {
    final case object Write extends TransactionType
    final case object Get   extends TransactionType
  }

  // Need to go through the chunk and make sure we don't have mixed transaction actions
  private[dynamodb] def filterMixedTransactions[A](
    actions: Chunk[Constructor[Any, A]]
  ): Either[Throwable, (Chunk[Constructor[Any, A]], TransactionType)] =
    if (actions.isEmpty) Left(EmptyTransaction())
    else {
      val headConstructor = constructorToTransactionType(actions.head)
        .map(Right(_))
        .getOrElse(Left(InvalidTransactionActions(NonEmptyChunk(actions.head)))) // TODO: Grab all that are invalid
      actions
        .drop(1)                                                                 // dropping 1 because we have the head element as the base case of the fold
        .foldLeft(headConstructor: Either[Throwable, TransactionType]) {
          case (acc, constructor) =>
            acc match {
              case l @ Left(_)            => l // Should also continue collecting other failures
              case Right(transactionType) => constructorMatch(constructor, transactionType)
            }
        }
        .map(transactionType => (actions, transactionType))
    }

  private def constructorMatch[A](
    constructor: Constructor[Any, A],
    transactionType: TransactionType
  ): Either[Throwable, TransactionType] =
    constructorToTransactionType(constructor)
      .map(t =>
        if (t == transactionType) Right(transactionType)
        else Left(MixedTransactionTypes())
      )
      .getOrElse(
        Left(InvalidTransactionActions(NonEmptyChunk(constructor)))
      )

  private def constructorToTransactionType[A](constructor: Constructor[_, A]): Option[TransactionType] =
    constructor match {
      case _: DeleteItem     => Some(TransactionType.Write)
      case _: PutItem        => Some(TransactionType.Write)
      case _: BatchWriteItem => Some(TransactionType.Write)
      case _: UpdateItem     => Some(TransactionType.Write)
      case _: ConditionCheck => Some(TransactionType.Write)
      case _: GetItem        => Some(TransactionType.Get)
      case _: BatchGetItem   => Some(TransactionType.Get)
      case _                 => None
    }

  private[dynamodb] def buildTransaction[A](
    query: DynamoDBQuery[_, A]
  ): Either[
    Throwable,
    (Chunk[Constructor[Any, Any]], Chunk[Any] => A)
  ] =
    query match {
      case constructor: Constructor[_, A] =>
        constructor match {
          case s: PutItem                  => Right((Chunk(s), chunk => chunk(0).asInstanceOf[A]))
          case s: DeleteItem               => Right((Chunk(s), chunk => chunk(0).asInstanceOf[A]))
          case s: GetItem                  =>
            Right(
              (
                Chunk(s),
                chunk => {
                  val maybeItem = chunk(0).asInstanceOf[Option[AttrMap]] // may have an empty AttrMap
                  maybeItem.flatMap(item => if (item.map.isEmpty) None else Some(item)).asInstanceOf[A]
                }
              )
            )
          case s: BatchGetItem             =>
            Right(
              (
                Chunk(s),
                chunk => {
                  val b: Seq[(TableName, Int)] = s.requestItems.toSeq.flatMap {
                    case (tName, items) => Seq.fill(items.keysSet.size)(tName)
                  }.zipWithIndex
                  val responses                = b.foldLeft(MapOfSet.empty[TableName, Item]) {
                    case (acc, (tableName, index)) =>
                      val maybeItem = chunk(index).asInstanceOf[Option[AttrMap]]
                      maybeItem match {
                        case Some(value) =>
                          if (value.map.isEmpty) // may have an empty AttrMap
                            acc
                          else acc.addAll((tableName, value))
                        case None        => acc
                      }
                  }

                  BatchGetItem.Response(responses = responses).asInstanceOf[A]
                }
              )
            )

          case s: BatchWriteItem           => Right((Chunk(s), _ => BatchWriteItem.Response(None).asInstanceOf[A]))
          case Transaction(query, _, _, _) => buildTransaction(query)
          case s: UpdateItem               =>
            Right(
              (Chunk(s), _ => None.asInstanceOf[A])
            ) // we don't get data back from transactWrites, return None here
          case s: ConditionCheck           => Right((Chunk(s), chunk => chunk(0).asInstanceOf[A]))
          case s                           => Left(InvalidTransactionActions(NonEmptyChunk(s.asInstanceOf[DynamoDBQuery[Any, Any]])))
        }
      case Zip(left, right, zippable)     =>
        for {
          l <- buildTransaction(left)
          r <- buildTransaction(right)
        } yield (
          l._1 ++ r._1,
          (chunk: Chunk[Any]) => {
            val leftChunk  = chunk.take(l._1.length)
            val rightChunk = chunk.drop(l._1.length)

            val leftValue  = l._2(leftChunk)
            val rightValue = r._2(rightChunk)

            zippable.zip(leftValue, rightValue)
          }
        )
      case Map(query, mapper)             =>
        buildTransaction(query).map {
          case (constructors, construct) => (constructors, chunk => mapper.asInstanceOf[Any => A](construct(chunk)))
        }
    }

  private[dynamodb] def constructTransaction[A](
    actions: Chunk[Constructor[Any, A]],
    transactionType: TransactionType,
    clientRequestToken: Option[String],
    itemCollectionMetrics: ReturnItemCollectionMetrics,
    returnConsumedCapacity: ReturnConsumedCapacity
  ): Either[TransactGetItemsRequest, TransactWriteItemsRequest] =
    transactionType match {
      case TransactionType.Write =>
        Right(constructWriteTransaction(actions, clientRequestToken, returnConsumedCapacity, itemCollectionMetrics))
      case TransactionType.Get   => Left(constructGetTransaction(actions, returnConsumedCapacity))
    }

  private[dynamodb] def constructGetTransaction[A](
    actions: Chunk[Constructor[Any, A]],
    returnConsumedCapacity: ReturnConsumedCapacity
  ): TransactGetItemsRequest = {
    val getActions: Chunk[TransactGetItem] = actions.flatMap {
      case s: GetItem      =>
        Some(
          TransactGetItem(
            Get(
              key = s.key.toZioAwsMap(),
              tableName = ZIOAwsTableName(s.tableName.value),
              projectionExpression =
                toOption(s.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_))
            )
          )
        )
      case s: BatchGetItem =>
        s.requestItems.flatMap {
          case (tableName, items) =>
            items.keysSet.map { key =>
              TransactGetItem(
                Get(
                  key = key.toZioAwsMap(),
                  tableName = ZIOAwsTableName(tableName.value),
                  projectionExpression = toOption(items.projectionExpressionSet)
                    .map(awsProjectionExpression)
                    .map(ZIOAwsProjectionExpression(_))
                )
              )
            }
        }
      case _               => None
    }
    TransactGetItemsRequest(
      transactItems = getActions,
      returnConsumedCapacity = Some(awsConsumedCapacity(returnConsumedCapacity))
    )
  }

  private[dynamodb] def constructWriteTransaction[A](
    actions: Chunk[Constructor[Any, A]],
    clientRequestToken: Option[String],
    returnConsumedCapacity: ReturnConsumedCapacity,
    itemCollectionMetrics: ReturnItemCollectionMetrics
  ): TransactWriteItemsRequest = {
    val writeActions: Chunk[TransactWriteItem] = actions.flatMap {
      case s: DeleteItem     => awsTransactWriteItem(s)
      case s: PutItem        => awsTransactWriteItem(s)
      case s: BatchWriteItem =>
        s.requestItems.flatMap {
          case (table, items) =>
            val something = items.map {
              case BatchWriteItem.Delete(key) =>
                TransactWriteItem(delete =
                  Some(
                    ZIOAwsDelete(
                      key = key.toZioAwsMap(),
                      tableName = ZIOAwsTableName(table.value)
                    )
                  )
                )
              case BatchWriteItem.Put(item)   =>
                TransactWriteItem(put =
                  Some(
                    ZIOAwsPut(
                      item = item.toZioAwsMap(),
                      tableName = ZIOAwsTableName(table.value)
                    )
                  )
                )
            }
            Chunk.fromIterable(something)
        }
      case s: UpdateItem     => awsTransactWriteItem(s)
      case s: ConditionCheck => awsTransactWriteItem(s)
      case _                 => None
    }

    TransactWriteItemsRequest(
      transactItems = writeActions,
      returnConsumedCapacity = Some(awsConsumedCapacity(returnConsumedCapacity)),
      returnItemCollectionMetrics = Some(awsReturnItemCollectionMetrics(itemCollectionMetrics)),
      clientRequestToken = clientRequestToken.map(ZIOAwsClientRequestToken(_))
    )
  }

  private val catchBatchRetryError: PartialFunction[Throwable, ZIO[Any, Throwable, Option[Nothing]]] = {
    case thrown: Throwable => ZIO.fail(thrown).unless(thrown.isInstanceOf[BatchRetryError])
  }

  private def optionalItem(updateItemResponse: UpdateItemResponse.ReadOnly): Option[Item] =
    updateItemResponse.attributes.toOption.flatMap(m => toOption(m).map(dynamoDBItem))

  private def aliasMapToExpressionZIOAwsAttributeValues(
    aliasMap: AliasMap
  ): Option[ScalaMap[ZIOAwsExpressionAttributeValueVariable, ZIOAwsAttributeValue]] =
    if (aliasMap.isEmpty) None
    else
      Some(aliasMap.map.flatMap {
        case (attrVal, str) =>
          val x: Option[(ZIOAwsExpressionAttributeValueVariable.Type, ZIOAwsAttributeValue)] =
            awsAttributeValue(attrVal).map(a => (ZIOAwsExpressionAttributeValueVariable(str), a))
          x
      })

  private[dynamodb] def tableGetToKeysAndAttributes(tableGet: TableGet): KeysAndAttributes = {
    val maybeProjectionExprn                             = toOption(tableGet.projectionExpressionSet).map(awsProjectionExpression)
    val (maybeAwsNamesMap, maybeProjectionExprnReplaced) = awsExprnAttrNamesAndReplaced(maybeProjectionExprn)
    val keys                                             = tableGet.keysSet.map(set => set.toZioAwsMap())
    println(
      s"TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT keys=$keys exprn=${maybeProjectionExprn} awsNamesMap=$maybeAwsNamesMap exprnReplaced=$maybeProjectionExprnReplaced"
    )

    KeysAndAttributes(
      keys = tableGet.keysSet.map(set => set.toZioAwsMap()),
//      projectionExpression = toOption(tableGet.projectionExpressionSet)
//        .map(awsProjectionExpression)
//        .map(ZIOAwsProjectionExpression(_)),
      projectionExpression = maybeProjectionExprnReplaced.map(ZIOAwsProjectionExpression(_)),
      expressionAttributeNames = maybeAwsNamesMap // TODO: Avi
    )
  }

  private[dynamodb] def writeRequestToBatchWrite(writeRequest: WriteRequest.ReadOnly): Option[BatchWriteItem.Write] =
    writeRequest.putRequest.toOption.map(put => BatchWriteItem.Put(item = AttrMap(awsAttrMapToAttrMap(put.item))))

  private def keysAndAttrsToTableGet(ka: KeysAndAttributes.ReadOnly): TableGet = {
    val maybeProjectionExpressions = ka.projectionExpression.map(
      _.split(",").map(ProjectionExpression.$).toSet
    ) // TODO(adam): Need a better way to accomplish this
    val keySet =
      ka.keys
        .map(m => PrimaryKey(m.flatMap { case (k, v) => awsAttrValToAttrVal(v).map((k.toString, _)) }))
        .toSet

    maybeProjectionExpressions
      .map(a => TableGet(keySet, a.asInstanceOf[Set[ProjectionExpression[_, _]]]))
      .getOrElse(TableGet(keySet, Set.empty))
  }

  private def tableItemsMapToResponse(
    tableItems: ScalaMap[TableName, List[ScalaMap[ZIOAwsAttributeName, ZIOAwsAttributeValue.ReadOnly]]]
  ) =
    tableItems.foldLeft(MapOfSet.empty[TableName, Item]) {
      case (acc, (tableName, list)) =>
        acc ++ ((tableName, list.map(dynamoDBItem)))
    }

  private def dynamoDBItem(attrMap: ScalaMap[ZIOAwsAttributeName, ZIOAwsAttributeValue.ReadOnly]): Item =
    Item(attrMap.flatMap { case (k, v) => awsAttrValToAttrVal(v).map(attrVal => (k.toString, attrVal)) })

  implicit class ToZioAwsMap(item: AttrMap) {
    def toZioAwsMap(): ScalaMap[ZIOAwsAttributeName, ZIOAwsAttributeValue] =
      item.map.flatMap { case (k, v) => awsAttributeValue(v).map(a => (ZIOAwsAttributeName(k.toString), a)) }
  }

  private def awsExprnAttrNamesAndReplaced2[A](
    escapedExpression: String
  )(f: String => A): (
    Option[ScalaMap[primitives.ExpressionAttributeNameVariable.Type, ZIOAwsAttributeName.Type]],
    A
  ) =
    (ReservedAttributeNames.parse(escapedExpression) match {
      case (map, replaced) =>
        if (map.isEmpty)
          (None, replaced)
        else
          (Some(map), replaced)
    }) match {
      case (Some(map), replaced) =>
        val awsReplaced = f(replaced)
        val awsMap      = map.map {
          case (k, v) =>
            println(s"XXXXXXXXXXXXXXXXXXXXXXX exprnAttrNamesAndReplaced k=$k")
            (ExpressionAttributeNameVariable(k), ZIOAwsAttributeName(v))
        }
        (Some(awsMap), awsReplaced)
      case (None, replaced)      =>
        val awsReplaced = f(replaced)
        (None, awsReplaced)
    }

  private def awsExprnAttrNamesAndReplaced(
    escapedExpression: String
  ): (
    Option[ScalaMap[primitives.ExpressionAttributeNameVariable.Type, ZIOAwsAttributeName.Type]],
    ZIOAwsConditionExpression
  ) =
    (ReservedAttributeNames.parse(escapedExpression) match {
      case (map, replaced) =>
        if (map.isEmpty)
          (None, replaced)
        else
          (Some(map), replaced)
    }) match {
      case (Some(map), replaced) =>
        val awsReplaced = ZIOAwsConditionExpression(replaced)
        val awsMap      = map.map {
          case (k, v) =>
            println(s"XXXXXXXXXXXXXXXXXXXXXXX exprnAttrNamesAndReplaced k=$k")
            (ExpressionAttributeNameVariable(k), ZIOAwsAttributeName(v))
        }
        (Some(awsMap), awsReplaced)
      case (None, replaced)      =>
        val awsReplaced = ZIOAwsConditionExpression(replaced)
        (None, awsReplaced)
    }

  // TODO: make private
  def awsExprnAttrNamesAndReplaced(
    maybeEscapedExpression: Option[String]
  ): (
    Option[ScalaMap[primitives.ExpressionAttributeNameVariable.Type, ZIOAwsAttributeName.Type]],
    Option[ZIOAwsConditionExpression]
  ) = {
    val mapAndReplaced =
      for {
        expression             <- maybeEscapedExpression
        (maybeAwsMap, replaced) = awsExprnAttrNamesAndReplaced(expression)
      } yield (maybeAwsMap, replaced)

    (mapAndReplaced.map(_._1).flatten, mapAndReplaced.map(_._2))
  }

  def awsExprnAttrNamesAndReplaced2[A](
    maybeEscapedExpression: Option[String]
  )(f: String => A): (
    Option[ScalaMap[primitives.ExpressionAttributeNameVariable.Type, ZIOAwsAttributeName.Type]],
    Option[A]
  ) = {
    val mapAndReplaced =
      for {
        expression             <- maybeEscapedExpression
        (maybeAwsMap, replaced) = awsExprnAttrNamesAndReplaced2(expression)(f)
      } yield (maybeAwsMap, replaced)

    (mapAndReplaced.map(_._1).flatten, mapAndReplaced.map(_._2))
  }

  def awsExprnAttrNamesAndReplaced(
    updateExprn: String,
    maybeEscapedConditionExpression: Option[String]
  ): (
    Option[ScalaMap[primitives.ExpressionAttributeNameVariable.Type, ZIOAwsAttributeName.Type]],
    ZIOAwsUpdateExpression,
    Option[ZIOAwsConditionExpression]
  ) = {
    val (map1, replacedUpdateExprn) = ReservedAttributeNames.parse(updateExprn)

    val maybeCondMapAndExprn: Option[(ScalaMap[String, String], String)] = {
      for {
        condExprn                <- maybeEscapedConditionExpression
        (map2, replacedCondExprn) = ReservedAttributeNames.parse(condExprn)
      } yield (
        map2,
        replacedCondExprn
      )
    }

    val mapFinal = maybeCondMapAndExprn.map(_._1).getOrElse(ScalaMap.empty) ++ map1
    val maybeAwsMap = {
      val m = toAwsNamesMap(mapFinal)
      if (m.isEmpty) None else Some(m)
    }

    (
      maybeAwsMap,
      ZIOAwsUpdateExpression(replacedUpdateExprn),
      maybeCondMapAndExprn.map(t => ZIOAwsConditionExpression(t._2))
    )
  }

  private def toAwsNamesMap(
    map: ScalaMap[String, String]
  ): ScalaMap[primitives.ExpressionAttributeNameVariable.Type, ZIOAwsAttributeName.Type] =
    map.map { // TODO: this should handle empty => None mapping as its an AWS concern
      case (k, v) =>
        (ExpressionAttributeNameVariable(k), ZIOAwsAttributeName(v))
    }

  private def awsPutItemRequest(putItem: PutItem): PutItemRequest = {
    val maybeAliasMap                         = putItem.conditionExpression.map(_.render.execute)
    val (maybeAwsNamesMap, maybeAwsCondition) = awsExprnAttrNamesAndReplaced(maybeAliasMap.map(_._2))
    PutItemRequest(
      tableName = ZIOAwsTableName(putItem.tableName.value),
      item = awsAttributeValueMap(putItem.item.map),
      returnConsumedCapacity = Some(awsConsumedCapacity(putItem.capacity)),
      returnItemCollectionMetrics = Some(ReturnItemCollectionMetrics.toZioAws(putItem.itemMetrics)),
      conditionExpression = maybeAwsCondition, //maybeCondExprn,
      // Optional[Map[ExpressionAttributeValueVariable, zio.aws.dynamodb.model.AttributeValue]]
      // eg ":v0" -> AV
      expressionAttributeValues = maybeAliasMap.flatMap(m => aliasMapToExpressionZIOAwsAttributeValues(m._1)),
      // Optional[Map[ExpressionAttributeNameVariable, AttributeName]]
      expressionAttributeNames = maybeAwsNamesMap,
      returnValues = Some(awsReturnValues(putItem.returnValues))
    )
  }

  private def awsGetItemRequest(getItem: GetItem): GetItemRequest = {
    println(s"XXXXXXXXXXXXXXXXXXXXXXXXXXXX getItem.projections=${getItem.projections}")
    GetItemRequest(
      tableName = ZIOAwsTableName(getItem.tableName.value),
      key = getItem.key.toZioAwsMap(),
      consistentRead = Some(ConsistencyMode.toBoolean(getItem.consistency)).map(ZIOAwsConsistentRead(_)),
      returnConsumedCapacity = Some(awsConsumedCapacity(getItem.capacity)),
      projectionExpression =
        toOption(getItem.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_))
    )
  }

  private[dynamodb] def awsBatchWriteItemRequest(batchWriteItem: BatchWriteItem): BatchWriteItemRequest =
    BatchWriteItemRequest(
      requestItems = batchWriteItem.requestItems.map {
        case (tableName, items) =>
          (ZIOAwsTableName(tableName.value), items.map(awsWriteRequest))
      }.toMap, // TODO(adam): MapOfSet uses iterable, maybe we should add a mapKeyValues?
      returnConsumedCapacity = Some(awsConsumedCapacity(batchWriteItem.capacity)),
      returnItemCollectionMetrics = Some(awsReturnItemCollectionMetrics(batchWriteItem.itemMetrics))
    )

  private[dynamodb] def awsBatchGetItemRequest(batchGetItem: BatchGetItem): BatchGetItemRequest =
    BatchGetItemRequest(
      requestItems = batchGetItem.requestItems.map {
        case (tableName, tableGet) =>
          (ZIOAwsTableName(tableName.value), tableGetToKeysAndAttributes(tableGet))
      },
      returnConsumedCapacity = Some(awsConsumedCapacity(batchGetItem.capacity))
    )

  private def awsDeleteItemRequest(deleteItem: DeleteItem): DeleteItemRequest = {
    // Option[(AliasMap, String)]
    val maybeAliasMap                         = deleteItem.conditionExpression.map(_.render.execute)
    val (maybeAwsNamesMap, maybeAwsCondition) = awsExprnAttrNamesAndReplaced(maybeAliasMap.map(_._2))
    DeleteItemRequest(
      tableName = ZIOAwsTableName(deleteItem.tableName.value),
      key = deleteItem.key.toZioAwsMap(),
      conditionExpression = maybeAwsCondition,
      expressionAttributeValues = maybeAliasMap.flatMap(m => aliasMapToExpressionZIOAwsAttributeValues(m._1)),
      expressionAttributeNames = maybeAwsNamesMap,
      returnConsumedCapacity = Some(awsConsumedCapacity(deleteItem.capacity)),
      returnItemCollectionMetrics = Some(awsReturnItemCollectionMetrics(deleteItem.itemMetrics)),
      returnValues = Some(awsReturnValues(deleteItem.returnValues))
    )
  }

  private def awsCreateTableRequest(createTable: CreateTable): CreateTableRequest =
    CreateTableRequest(
      attributeDefinitions = createTable.attributeDefinitions.map(awsAttributeDefinition),
      tableName = ZIOAwsTableName(createTable.tableName.value),
      keySchema = awsKeySchema(createTable.keySchema),
      localSecondaryIndexes = toOption(
        createTable.localSecondaryIndexes.map(awsLocalSecondaryIndex).toList
      ),
      globalSecondaryIndexes = toOption(
        createTable.globalSecondaryIndexes.map(awsGlobalSecondaryIndex).toList
      ),
      billingMode = Some(awsBillingMode(createTable.billingMode)),
      provisionedThroughput = createTable.billingMode match {
        case BillingMode.Provisioned(provisionedThroughput) =>
          Some(
            ZIOAwsProvisionedThroughput(
              readCapacityUnits = PositiveLongObject(provisionedThroughput.readCapacityUnit),
              writeCapacityUnits = PositiveLongObject(provisionedThroughput.writeCapacityUnit)
            )
          )
        case BillingMode.PayPerRequest                      => None
      },
      sseSpecification = createTable.sseSpecification.map(awsSSESpecification),
      tags = Some(createTable.tags.map { case (k, v) => Tag(TagKeyString(k), TagValueString(v)) })
    )

  private def awsUpdateItemRequest(updateItem: UpdateItem): UpdateItemRequest = {
    // (aliasMap, String, Option[String])
    val (aliasMap, (updateExpr, maybeConditionExpr))          = (for {
      updateExpr    <- updateItem.updateExpression.render
      conditionExpr <- AliasMapRender.collectAll(updateItem.conditionExpression.map(_.render))
    } yield (updateExpr, conditionExpr)).execute
    val (maybeAwsNamesMap, awsUpdateExprn, maybeAwsCondition) =
      awsExprnAttrNamesAndReplaced(updateExpr, maybeConditionExpr)

    UpdateItemRequest(
      tableName = ZIOAwsTableName(updateItem.tableName.value),
      key = updateItem.key.toZioAwsMap(),
      returnValues = Some(awsReturnValues(updateItem.returnValues)),
      returnConsumedCapacity = Some(awsConsumedCapacity(updateItem.capacity)),
      returnItemCollectionMetrics = Some(awsReturnItemCollectionMetrics(updateItem.itemMetrics)),
      updateExpression = Some(awsUpdateExprn),
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap).map(_.map {
        case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v)
      }),
      expressionAttributeNames = maybeAwsNamesMap,
      conditionExpression = maybeAwsCondition
    )
  }

  private def awsQueryRequest(queryAll: QueryAll): QueryRequest = {
    // (AliasMap, (Option[String], Option[String]))
    val (aliasMap, (maybeFilterExpr, maybeKeyExpr)) = (for {
      filter  <- AliasMapRender.collectAll(queryAll.filterExpression.map(_.render))
      keyExpr <- AliasMapRender.collectAll(queryAll.keyConditionExpression.map(_.render))
    } yield (filter, keyExpr)).execute
    val mapAndExprn                                 = awsExprnAttrNamesAndReplaced(maybeFilterExpr)
    val maybeProjectionExpressions                  = toOption(queryAll.projections).map(awsProjectionExpression)
    val mapAndProjectionExprn                       = awsExprnAttrNamesAndReplaced2(maybeProjectionExpressions)(ZIOAwsProjectionExpression(_))
    val awsNamesMap                                 = toOption(
      mapAndExprn._1.getOrElse(ScalaMap.empty) ++ mapAndProjectionExprn._1.getOrElse(ScalaMap.empty)
    )

    QueryRequest(
      tableName = ZIOAwsTableName(queryAll.tableName.value),
      indexName = queryAll.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = queryAll.select.map(awsSelect),
      limit = queryAll.limit.map(PositiveIntegerObject(_)),
      consistentRead = Some(toBoolean(queryAll.consistency)).map(ZIOAwsConsistentRead(_)),
      scanIndexForward = Some(queryAll.ascending),
      exclusiveStartKey = queryAll.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      projectionExpression = mapAndProjectionExprn._2,
//        toOption(queryAll.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      returnConsumedCapacity = Some(awsConsumedCapacity(queryAll.capacity)),
      filterExpression = mapAndExprn._2,
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap).map(m =>
        m.map { case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v) }
      ),
      expressionAttributeNames = awsNamesMap,
      keyConditionExpression = maybeKeyExpr.map(ZIOAwsKeyExpression(_))
    )
  }

  private def awsQueryRequest(querySome: QuerySome): QueryRequest = {
    // (AliasMap, (Option[String], Option[String]))
    val (aliasMap, (maybeFilterExpr, maybeKeyExpr)) = (for {
      filter  <- AliasMapRender.collectAll(querySome.filterExpression.map(_.render))
      keyExpr <- AliasMapRender.collectAll(querySome.keyConditionExpression.map(_.render))
    } yield (filter, keyExpr)).execute
    val mapAndExprn                                 = awsExprnAttrNamesAndReplaced(maybeFilterExpr)
    val maybeProjectionExpressions                  = toOption(querySome.projections).map(awsProjectionExpression)
    val mapAndProjectionExprn                       = awsExprnAttrNamesAndReplaced2(maybeProjectionExpressions)(ZIOAwsProjectionExpression(_))
    val awsNamesMap                                 = toOption(
      mapAndExprn._1.getOrElse(ScalaMap.empty) ++ mapAndProjectionExprn._1.getOrElse(ScalaMap.empty)
    )

    QueryRequest(
      tableName = ZIOAwsTableName(querySome.tableName.value),
      indexName = querySome.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = querySome.select.map(awsSelect),
      limit = Some(querySome.limit).map(PositiveIntegerObject(_)),
      consistentRead = Some(toBoolean(querySome.consistency)).map(ZIOAwsConsistentRead(_)),
      scanIndexForward = Some(querySome.ascending),
      exclusiveStartKey = querySome.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      returnConsumedCapacity = Some(awsConsumedCapacity(querySome.capacity)),
      projectionExpression = mapAndProjectionExprn._2,
//        toOption(querySome.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      filterExpression = mapAndExprn._2, //maybeFilterExpr.map(ZIOAwsConditionExpression(_)),
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap).map(_.map {
        case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v)
      }),
      expressionAttributeNames = awsNamesMap,
      keyConditionExpression = maybeKeyExpr.map(ZIOAwsKeyExpression(_))
    )
  }
  private def awsScanRequest(scanAll: ScanAll, segment: Option[ScanAll.Segment]): ScanRequest = {
    val filterExpression: Option[(AliasMap, String)] = scanAll.filterExpression.map(fe => fe.render.execute)
    val (maybeNames, maybeCondExprn)                 = awsExprnAttrNamesAndReplaced(filterExpression.map(_._2))

    ScanRequest(
      tableName = ZIOAwsTableName(scanAll.tableName.value),
      indexName = scanAll.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = scanAll.select.map(awsSelect),
      exclusiveStartKey = scanAll.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      returnConsumedCapacity = Some(awsConsumedCapacity(scanAll.capacity)),
      limit = scanAll.limit.map(PositiveIntegerObject(_)),
      projectionExpression =
        toOption(scanAll.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      filterExpression =
        maybeCondExprn, //zioAwsFilterExpression, // filterExpression.map(_._2).map(ZIOAwsConditionExpression(_)),
      expressionAttributeValues = filterExpression
        .flatMap(a => aliasMapToExpressionZIOAwsAttributeValues(a._1))
        .map(_.map { case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v) }),
      expressionAttributeNames = maybeNames,
      consistentRead = Some(toBoolean(scanAll.consistency)).map(ZIOAwsConsistentRead(_)),
      totalSegments = segment.map(_.total).map(ScanTotalSegments(_)),
      segment = segment.map(_.number).map(ScanSegment(_))
    )
  }

  private def awsScanRequest(scanSome: ScanSome): ScanRequest = {
    // Option[(AliasMap, String)]
    val filterExpression             = scanSome.filterExpression.map(fe => fe.render.execute)
    val (maybeNames, maybeCondExprn) = awsExprnAttrNamesAndReplaced(filterExpression.map(_._2))
    ScanRequest(
      tableName = ZIOAwsTableName(scanSome.tableName.value),
      indexName = scanSome.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = scanSome.select.map(awsSelect),
      exclusiveStartKey = scanSome.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      returnConsumedCapacity = Some(awsConsumedCapacity(scanSome.capacity)),
      limit = Some(scanSome.limit).map(PositiveIntegerObject(_)),
      projectionExpression =
        toOption(scanSome.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      filterExpression = maybeCondExprn,
      expressionAttributeValues = filterExpression
        .flatMap(a => aliasMapToExpressionZIOAwsAttributeValues(a._1))
        .map(_.map { case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v) }),
      expressionAttributeNames = maybeNames,
      consistentRead = Some(toBoolean(scanSome.consistency)).map(ZIOAwsConsistentRead(_))
    )
  }

  private def awsTransactWriteItem[A](action: Constructor[_, A]): Option[TransactWriteItem] =
    action match {
      case conditionCheck: ConditionCheck =>
        Some(TransactWriteItem(conditionCheck = Some(awsConditionCheck(conditionCheck))))
      case put: PutItem                   => Some(TransactWriteItem(put = Some(awsTransactPutItem(put))))
      case delete: DeleteItem             => Some(TransactWriteItem(delete = Some(awsTransactDeleteItem(delete))))
      case update: UpdateItem             => Some(TransactWriteItem(update = Some(awsTransactUpdateItem(update))))
      case _                              => None
    }

  private def awsConditionCheck(conditionCheck: ConditionCheck): ZIOAwsConditionCheck = {
    // (AliasMap, String)
    val (aliasMap, conditionExpression)      = conditionCheck.conditionExpression.render.execute
    val (maybeAwsNameMap, awsSubstCondition) = awsExprnAttrNamesAndReplaced(conditionExpression)

    ZIOAwsConditionCheck(
      key = conditionCheck.primaryKey.toZioAwsMap(),
      tableName = ZIOAwsTableName(conditionCheck.tableName.value),
      conditionExpression = awsSubstCondition,
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap),
      expressionAttributeNames = maybeAwsNameMap
    )
  }

  private def awsTransactPutItem(put: PutItem): ZIOAwsPut = {
    // (AliasMap, String)
    val (aliasMap, conditionExpression)           = AliasMapRender.collectAll(put.conditionExpression.map(_.render)).execute
    val (maybeAwsNameMap, maybeAwsSubstCondition) = awsExprnAttrNamesAndReplaced(conditionExpression)

    ZIOAwsPut(
      item = put.item.toZioAwsMap(),
      tableName = ZIOAwsTableName(put.tableName.value),
      conditionExpression = maybeAwsSubstCondition,
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap),
      expressionAttributeNames = maybeAwsNameMap
    )
  }

  private def awsTransactDeleteItem(delete: DeleteItem): ZIOAwsDelete = {
    // (AliasMap, Option[String])
    val (aliasMap, conditionExpression)           = AliasMapRender.collectAll(delete.conditionExpression.map(_.render)).execute
    val (maybeAwsNameMap, maybeAwsSubstCondition) = awsExprnAttrNamesAndReplaced(conditionExpression)
    println(
      s"ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ awsSubstCondition=$maybeAwsSubstCondition maybeAwsNameMap=$maybeAwsNameMap"
    )

    ZIOAwsDelete(
      key = delete.key.toZioAwsMap(),
      tableName = ZIOAwsTableName(delete.tableName.value),
      conditionExpression = maybeAwsSubstCondition,
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap),
      expressionAttributeNames = maybeAwsNameMap
    )
  }

  private def awsTransactUpdateItem(update: UpdateItem): ZIOAwsUpdate = {
    // (AliasMap, (String, Option[String]))
    val (aliasMap, (updateExpr, maybeConditionExpr))          = (for {
      updateExpr    <- update.updateExpression.render
      conditionExpr <- AliasMapRender.collectAll(update.conditionExpression.map(_.render))
    } yield (updateExpr, conditionExpr)).execute
    val (maybeAwsNamesMap, awsUpdateExprn, maybeAwsCondition) =
      awsExprnAttrNamesAndReplaced(updateExpr, maybeConditionExpr)

    ZIOAwsUpdate(
      key = update.key.toZioAwsMap(),
      tableName = ZIOAwsTableName(update.tableName.value),
      conditionExpression = maybeAwsCondition,
      updateExpression = awsUpdateExprn,
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap),
      expressionAttributeNames = maybeAwsNamesMap
    )
  }

  private[dynamodb] def awsProjectionExpression(
    projectionExpressions: Iterable[ProjectionExpression[_, _]]
  ): String =
    projectionExpressions.mkString(", ")

  private[dynamodb] def awsAttributeValueMap(
    attrMap: ScalaMap[String, AttributeValue]
  ): ScalaMap[ZIOAwsAttributeName, ZIOAwsAttributeValue]                                                 =
    attrMap.flatMap { case (k, v) => awsAttributeValue(v).map(a => (ZIOAwsAttributeName(k), a)) }

  private def awsAttrValToAttrVal(attributeValue: ZIOAwsAttributeValue.ReadOnly): Option[AttributeValue] =
    attributeValue.s
      .map(AttributeValue.String)
      .orElse {
        attributeValue.n.map(n => AttributeValue.Number(BigDecimal(n)))
      } // TODO(adam): Does the BigDecimal need a try wrapper?
      .orElse {
        attributeValue.b.map(b => AttributeValue.Binary(b))
      }
      .orElse {
        attributeValue.ns.flatMap(ns => toOption(ns).map(ns => AttributeValue.NumberSet(ns.map(BigDecimal(_)).toSet)))
        // TODO(adam): Wrap in try?
      }
      .orElse {
        attributeValue.ss.flatMap(s =>
          toOption(s).map(a => AttributeValue.StringSet(a.toSet))
        ) // TODO(adam): Is this `toSet` actually safe to do?
      }
      .orElse {
        attributeValue.bs.flatMap(bs => toOption(bs).map(bs => AttributeValue.BinarySet(bs.toSet)))
      }
      .orElse {
        attributeValue.m.flatMap(m =>
          toOption(m).map(m =>
            AttributeValue.Map(
              m.flatMap {
                case (k, v) =>
                  awsAttrValToAttrVal(v).map(attrVal => (AttributeValue.String(k), attrVal))
              }
            )
          )
        )
      }
      .orElse {
        attributeValue.l.flatMap(l =>
          toOption(l).map(l => AttributeValue.List(Chunk.fromIterable(l.flatMap(awsAttrValToAttrVal))))
        )
      }
      .orElse(attributeValue.nul.map(_ => AttributeValue.Null))
      .orElse(attributeValue.bool.map(AttributeValue.Bool))
      .toOption

  private def awsReturnItemCollectionMetrics(metrics: ReturnItemCollectionMetrics): ZIOAwsReturnItemCollectionMetrics =
    metrics match {
      case ReturnItemCollectionMetrics.None => ZIOAwsReturnItemCollectionMetrics.NONE
      case ReturnItemCollectionMetrics.Size => ZIOAwsReturnItemCollectionMetrics.SIZE
    }

  private def dynamoDBTableStatus(tableStatus: ZIOAwsTableStatus): TableStatus =
    tableStatus match {
      case ZIOAwsTableStatus.CREATING                            => TableStatus.Creating
      case ZIOAwsTableStatus.UPDATING                            => TableStatus.Updating
      case ZIOAwsTableStatus.DELETING                            => TableStatus.Deleting
      case ZIOAwsTableStatus.ACTIVE                              => TableStatus.Active
      case ZIOAwsTableStatus.INACCESSIBLE_ENCRYPTION_CREDENTIALS => TableStatus.InaccessibleEncryptionCredentials
      case ZIOAwsTableStatus.ARCHIVING                           => TableStatus.Archiving
      case ZIOAwsTableStatus.ARCHIVED                            => TableStatus.Archived
      case ZIOAwsTableStatus.unknownToSdkVersion                 => TableStatus.unknownToSdkVersion
    }

  private def awsAttrMapToAttrMap(
    attrMap: ScalaMap[ZIOAwsAttributeName, ZIOAwsAttributeValue.ReadOnly]
  ): ScalaMap[String, AttributeValue] =
    attrMap.flatMap { case (k, v) => awsAttrValToAttrVal(v).map((k, _)) }

  private def awsConsumedCapacity(
    returnConsumedCapacity: ReturnConsumedCapacity
  ): ZIOAwsReturnConsumedCapacity     =
    returnConsumedCapacity match {
      case ReturnConsumedCapacity.Indexes => ZIOAwsReturnConsumedCapacity.INDEXES
      case ReturnConsumedCapacity.Total   => ZIOAwsReturnConsumedCapacity.TOTAL
      case ReturnConsumedCapacity.None    => ZIOAwsReturnConsumedCapacity.NONE
    }

  private def awsReturnValues(
    returnValues: ReturnValues
  ): ReturnValue =
    returnValues match {
      case ReturnValues.None       => ReturnValue.NONE
      case ReturnValues.AllOld     => ReturnValue.ALL_OLD
      case ReturnValues.UpdatedOld => ReturnValue.UPDATED_OLD
      case ReturnValues.AllNew     => ReturnValue.ALL_NEW
      case ReturnValues.UpdatedNew => ReturnValue.UPDATED_NEW
    }

  private def awsSelect(
    select: Select
  ): ZIOAwsSelect =
    select match {
      case Select.AllAttributes          => ZIOAwsSelect.ALL_ATTRIBUTES
      case Select.AllProjectedAttributes => ZIOAwsSelect.ALL_PROJECTED_ATTRIBUTES
      case Select.SpecificAttributes     => ZIOAwsSelect.SPECIFIC_ATTRIBUTES
      case Select.Count                  => ZIOAwsSelect.COUNT
    }

  private def awsBillingMode(
    billingMode: BillingMode
  ): ZIOAwsBillingMode =
    billingMode match {
      case BillingMode.Provisioned(_) => ZIOAwsBillingMode.PROVISIONED
      case BillingMode.PayPerRequest  => ZIOAwsBillingMode.PAY_PER_REQUEST
    }

  private def awsAttributeDefinition(attributeDefinition: AttributeDefinition): ZIOAwsAttributeDefinition =
    ZIOAwsAttributeDefinition(
      attributeName = ZIOAwsKeySchemaAttributeName(attributeDefinition.name),
      attributeType = attributeDefinition.attributeType match {
        case AttributeValueType.Binary => ScalarAttributeType.B
        case AttributeValueType.Number => ScalarAttributeType.N
        case AttributeValueType.String => ScalarAttributeType.S
      }
    )

  private def awsGlobalSecondaryIndex(globalSecondaryIndex: GlobalSecondaryIndex): ZIOAwsGlobalSecondaryIndex =
    ZIOAwsGlobalSecondaryIndex(
      indexName = ZIOAwsIndexName(globalSecondaryIndex.indexName),
      keySchema = awsKeySchema(globalSecondaryIndex.keySchema),
      projection = awsProjectionType(globalSecondaryIndex.projection),
      provisionedThroughput = globalSecondaryIndex.provisionedThroughput.map(provisionedThroughput =>
        ZIOAwsProvisionedThroughput(
          readCapacityUnits = PositiveLongObject(provisionedThroughput.readCapacityUnit),
          writeCapacityUnits = PositiveLongObject(provisionedThroughput.writeCapacityUnit)
        )
      )
    )

  private def awsLocalSecondaryIndex(localSecondaryIndex: LocalSecondaryIndex): ZIOAwsLocalSecondaryIndex =
    ZIOAwsLocalSecondaryIndex(
      indexName = ZIOAwsIndexName(localSecondaryIndex.indexName),
      keySchema = awsKeySchema(localSecondaryIndex.keySchema),
      projection = awsProjectionType(localSecondaryIndex.projection)
    )

  private def awsProjectionType(projectionType: ProjectionType): ZIOAwsProjection =
    projectionType match {
      case ProjectionType.KeysOnly                  =>
        ZIOAwsProjection(projectionType = Some(ZIOAwsProjectionType.KEYS_ONLY), nonKeyAttributes = None)
      case ProjectionType.Include(nonKeyAttributes) =>
        ZIOAwsProjection(
          projectionType = Some(ZIOAwsProjectionType.INCLUDE),
          nonKeyAttributes = Some(nonKeyAttributes.map(NonKeyAttributeName(_)))
        )
      case ProjectionType.All                       => ZIOAwsProjection(projectionType = Some(ZIOAwsProjectionType.ALL), None)
    }

  private def awsSSESpecification(sseSpecification: SSESpecification): ZIOAwsSSESpecification =
    ZIOAwsSSESpecification(
      enabled = Some(sseSpecification.enable).map(SSEEnabled(_)),
      sseType = Some(awsSSEType(sseSpecification.sseType)),
      kmsMasterKeyId = sseSpecification.kmsMasterKeyId.map(KMSMasterKeyId(_))
    )

  private def awsSSEType(sseType: SSEType): ZIOAwsSSEType =
    sseType match {
      case SSESpecification.AES256 => ZIOAwsSSEType.AES256
      case SSESpecification.KMS    => ZIOAwsSSEType.KMS
    }

  private def awsKeySchema(keySchema: KeySchema): List[KeySchemaElement] = {
    val hashKeyElement = List(
      KeySchemaElement(
        attributeName = ZIOAwsKeySchemaAttributeName(keySchema.hashKey),
        keyType = KeyType.HASH
      )
    )
    keySchema.sortKey.fold(hashKeyElement)(sortKey =>
      hashKeyElement :+ KeySchemaElement(attributeName = ZIOAwsKeySchemaAttributeName(sortKey), keyType = KeyType.RANGE)
    )
  }

  private def awsAttributeValue(
    attributeVal: AttributeValue
  ): Option[ZIOAwsAttributeValue] =
    attributeVal match {
      case AttributeValue.Binary(value)    =>
        Some(ZIOAwsAttributeValue(b = Some(Chunk.fromIterable(value)).map(BinaryAttributeValue(_))))
      case AttributeValue.BinarySet(value) =>
        if (value.isEmpty) None
        else Some(ZIOAwsAttributeValue(bs = Some(value.map(Chunk.fromIterable).map(BinaryAttributeValue(_)))))
      case AttributeValue.Bool(value)      => Some(ZIOAwsAttributeValue(bool = Some(value).map(BooleanAttributeValue(_))))
      case AttributeValue.List(value)      => Some(ZIOAwsAttributeValue(l = Some(value.flatMap(awsAttributeValue))))
      case AttributeValue.Map(value)       =>
        Some(ZIOAwsAttributeValue(m = Some(value.flatMap {
          case (k, v) => awsAttributeValue(v).map(r => (ZIOAwsAttributeName(k.value), r))
        })))
      case AttributeValue.Number(value)    =>
        Some(ZIOAwsAttributeValue(n = Some(value.toString()).map(NumberAttributeValue(_))))
      case AttributeValue.NumberSet(value) =>
        if (value.isEmpty) None
        else Some(ZIOAwsAttributeValue(ns = Some(value.map(_.toString()).map(NumberAttributeValue(_)))))
      case AttributeValue.Null             => Some(ZIOAwsAttributeValue(nul = Some(true).map(NullAttributeValue(_))))
      case AttributeValue.String(value)    => Some(ZIOAwsAttributeValue(s = Some(value).map(StringAttributeValue(_))))
      case AttributeValue.StringSet(value) =>
        if (value.isEmpty) None else Some(ZIOAwsAttributeValue(ss = Some(value.map(StringAttributeValue(_)))))
    }

  private[dynamodb] def awsWriteRequest(write: BatchWriteItem.Write): WriteRequest =
    write match {
      case BatchWriteItem.Delete(key) => WriteRequest(None, Some(DeleteRequest(key.toZioAwsMap())))
      case BatchWriteItem.Put(item)   => WriteRequest(Some(PutRequest(item.toZioAwsMap())), None)
    }

  private def toOption[A](list: List[A]): Option[::[A]] =
    list match {
      case Nil          => None
      case head :: tail => Some(::(head, tail))
    }

  private def toOption[A](set: Set[A]): Option[Set[A]] =
    if (set.isEmpty) None else Some(set)

  private def toOption[A, B](map: ScalaMap[A, B]): Option[ScalaMap[A, B]] =
    if (map.isEmpty) None else Some(map)

  private def mapOfListToMapOfSet[A, B](map: ScalaMap[TableName, List[A]])(f: A => Option[B]): MapOfSet[TableName, B] =
    map.foldLeft(MapOfSet.empty[TableName, B]) {
      case (acc, (tableName, l)) =>
        acc ++ ((tableName, l.map(f).flatten)) // TODO: Better way to make this compatible with 2.12 & 2.13?
    }
}
