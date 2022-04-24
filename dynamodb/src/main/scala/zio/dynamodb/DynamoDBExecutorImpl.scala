package zio.dynamodb
import zio.aws.core.FieldIsNone
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.primitives.{
  BinaryAttributeValue,
  BooleanAttributeValue,
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
  UpdateItemRequest,
  UpdateItemResponse,
  WriteRequest,
  AttributeDefinition => ZIOAwsAttributeDefinition,
  AttributeValue => ZIOAwsAttributeValue,
  BillingMode => ZIOAwsBillingMode,
  GlobalSecondaryIndex => ZIOAwsGlobalSecondaryIndex,
  LocalSecondaryIndex => ZIOAwsLocalSecondaryIndex,
  Projection => ZIOAwsProjection,
  ProjectionType => ZIOAwsProjectionType,
  ProvisionedThroughput => ZIOAwsProvisionedThroughput,
  ReturnConsumedCapacity => ZIOAwsReturnConsumedCapacity,
  ReturnItemCollectionMetrics => ZIOAwsReturnItemCollectionMetrics,
  SSESpecification => ZIOAwsSSESpecification,
  SSEType => ZIOAwsSSEType,
  Select => ZIOAwsSelect,
  TableStatus => ZIOAwsTableStatus
}
import zio.dynamodb.ConsistencyMode.toBoolean
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.SSESpecification.SSEType
import zio.stream.{ Stream, ZSink, ZStream }
import zio.{ Chunk, ZIO }

import scala.collection.immutable.{ Map => ScalaMap }

private[dynamodb] final case class DynamoDBExecutorImpl private (dynamoDb: DynamoDb) extends DynamoDBExecutor {
  import DynamoDBExecutorImpl._

  def executeMap[A, B](map: Map[A, B]): ZIO[Any, Throwable, B] =
    execute(map.query).map(map.mapper)

  def executeZip[A, B, C](zip: Zip[A, B, C]): ZIO[Any, Throwable, C] =
    execute(zip.left).zipWith(execute(zip.right))(zip.zippable.zip)

  def executeConstructor[A](constructor: Constructor[A]): ZIO[Any, Throwable, A] =
    constructor match {
      case c: GetItem        => executeGetItem(c)
      case c: PutItem        => executePutItem(c)
      case c: BatchGetItem   => executeBatchGetItem(c)
      case c: BatchWriteItem => executeBatchWriteItem(c)
      case c: ScanAll        => executeScanAll(c)
      case c: ScanSome       => executeScanSome(c)
      case c: UpdateItem     => executeUpdateItem(c)
      case c: CreateTable    => executeCreateTable(c)
      case c: DeleteItem     => executeDeleteItem(c)
      case c: DeleteTable    => executeDeleteTable(c)
      case c: DescribeTable  => executeDescribeTable(c)
      case c: QuerySome      => executeQuerySome(c)
      case c: QueryAll       => executeQueryAll(c)
      case Succeed(c)        => ZIO.succeed(c())
    }

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Throwable, A] =
    atomicQuery match {
      case constructor: Constructor[_] => executeConstructor(constructor)
      case zip @ Zip(_, _, _)          => executeZip(zip)
      case map @ Map(_, _)             => executeMap(map)
    }

  private def executeCreateTable(createTable: CreateTable): ZIO[Any, Throwable, Unit] =
    dynamoDb
      .createTable(awsCreateTableRequest(createTable))
      .mapError(_.toThrowable)
      .unit

  private def executeDeleteItem(deleteItem: DeleteItem): ZIO[Any, Throwable, Unit] =
    dynamoDb.deleteItem(awsDeleteItemRequest(deleteItem)).mapError(_.toThrowable).unit

  private def executeDeleteTable(deleteTable: DeleteTable): ZIO[Any, Throwable, Unit] =
    dynamoDb
      .deleteTable(DeleteTableRequest(ZIOAwsTableName(deleteTable.tableName.value)))
      .mapError(_.toThrowable)
      .unit

  private def executePutItem(putItem: PutItem): ZIO[Any, Throwable, Unit] =
    dynamoDb.putItem(awsPutItemRequest(putItem)).unit.mapError(_.toThrowable)

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
      .query(awsQueryRequest(querySome))
      .take(querySome.limit.toLong)
      .mapBoth(_.toThrowable, dynamoDBItem)
      .run(ZSink.collectAll[Item])
      .map(chunk => (chunk, chunk.lastOption))

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

  private def executeScanSome(scanSome: ScanSome): ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] =
    dynamoDb
      .scan(awsScanRequest(scanSome))
      .take(scanSome.limit.toLong)
      .mapBoth(_.toThrowable, dynamoDBItem)
      .run(ZSink.collectAll[Item])
      .map(chunk => (chunk, chunk.lastOption))

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

  private val catchBatchRetryError: PartialFunction[Throwable, ZIO[Any, Throwable, Option[Nothing]]] = {
    case thrown: Throwable => ZIO.fail(thrown).unless(thrown.isInstanceOf[BatchRetryError])
  }

  private def optionalItem(updateItemResponse: UpdateItemResponse.ReadOnly): Option[Item] =
    updateItemResponse.attributes.toOption.flatMap(m => toOption(m).map(dynamoDBItem))

  private def aliasMapToExpressionZIOAwsAttributeValues(
    aliasMap: AliasMap
  ): Option[ScalaMap[String, ZIOAwsAttributeValue]] =
    if (aliasMap.isEmpty) None
    else
      Some(aliasMap.map.map {
        case (attrVal, str) => (str, awsAttributeValue(attrVal))
      })

  private[dynamodb] def tableGetToKeysAndAttributes(tableGet: TableGet): KeysAndAttributes =
    KeysAndAttributes(
      keys = tableGet.keysSet.map(set =>
        set.map.map {
          case (k, v) =>
            (ZIOAwsAttributeName(k), awsAttributeValue(v))
        }
      ),
      projectionExpression = toOption(tableGet.projectionExpressionSet)
        .map(awsProjectionExpression)
        .map(ZIOAwsProjectionExpression(_))
    )

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
      .map(a => TableGet(keySet, a.asInstanceOf[Set[ProjectionExpression]]))
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

  private def awsPutItemRequest(putItem: PutItem): PutItemRequest                                       =
    PutItemRequest(
      tableName = ZIOAwsTableName(putItem.tableName.value),
      item = awsAttributeValueMap(putItem.item.map),
      returnConsumedCapacity = Some(awsConsumedCapacity(putItem.capacity)),
      returnItemCollectionMetrics = Some(ReturnItemCollectionMetrics.toZioAws(putItem.itemMetrics)),
      conditionExpression = putItem.conditionExpression.map(v => ZIOAwsConditionExpression(v.toString)),
      returnValues = Some(awsReturnValues(putItem.returnValues))
    )

  private def awsGetItemRequest(getItem: GetItem): GetItemRequest =
    GetItemRequest(
      tableName = ZIOAwsTableName(getItem.tableName.value),
      key = getItem.key.map.map { case (k, v) => (ZIOAwsAttributeName(k), awsAttributeValue(v)) },
      consistentRead = Some(ConsistencyMode.toBoolean(getItem.consistency)).map(ZIOAwsConsistentRead(_)),
      returnConsumedCapacity = Some(awsConsumedCapacity(getItem.capacity)),
      projectionExpression =
        toOption(getItem.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_))
    )

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

  private def awsDeleteItemRequest(deleteItem: DeleteItem): DeleteItemRequest =
    DeleteItemRequest(
      tableName = ZIOAwsTableName(deleteItem.tableName.value),
      key = deleteItem.key.map.map { case (k, v) => (ZIOAwsAttributeName(k), awsAttributeValue(v)) },
      conditionExpression = deleteItem.conditionExpression.map(_.toString).map(ZIOAwsConditionExpression(_)),
      returnConsumedCapacity = Some(awsConsumedCapacity(deleteItem.capacity)),
      returnItemCollectionMetrics = Some(awsReturnItemCollectionMetrics(deleteItem.itemMetrics)),
      returnValues = Some(awsReturnValues(deleteItem.returnValues))
    )

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
    val (aliasMap, (updateExpr, maybeConditionExpr)) = (for {
      updateExpr    <- updateItem.updateExpression.render
      conditionExpr <- AliasMapRender.collectAll(updateItem.conditionExpression.map(_.render))
    } yield (updateExpr, conditionExpr)).execute

    UpdateItemRequest(
      tableName = ZIOAwsTableName(updateItem.tableName.value),
      key = updateItem.key.map.map { case (k, v) => (ZIOAwsAttributeName(k), awsAttributeValue(v)) },
      returnValues = Some(awsReturnValues(updateItem.returnValues)),
      returnConsumedCapacity = Some(awsConsumedCapacity(updateItem.capacity)),
      returnItemCollectionMetrics = Some(awsReturnItemCollectionMetrics(updateItem.itemMetrics)),
      updateExpression = Some(updateExpr).map(ZIOAwsUpdateExpression(_)),
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap).map(_.map {
        case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v)
      }),
      conditionExpression = maybeConditionExpr.map(ZIOAwsConditionExpression(_))
    )
  }

  private def awsQueryRequest(queryAll: QueryAll): QueryRequest = {
    val (aliasMap, (maybeFilterExpr, maybeKeyExpr)) = (for {
      filter  <- AliasMapRender.collectAll(queryAll.filterExpression.map(_.render))
      keyExpr <- AliasMapRender.collectAll(queryAll.keyConditionExpression.map(_.render))
    } yield (filter, keyExpr)).execute

    QueryRequest(
      tableName = ZIOAwsTableName(queryAll.tableName.value),
      indexName = queryAll.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = queryAll.select.map(awsSelect),
      limit = queryAll.limit.map(PositiveIntegerObject(_)),
      consistentRead = Some(toBoolean(queryAll.consistency)).map(ZIOAwsConsistentRead(_)),
      scanIndexForward = Some(queryAll.ascending),
      exclusiveStartKey = queryAll.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      projectionExpression =
        toOption(queryAll.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      returnConsumedCapacity = Some(awsConsumedCapacity(queryAll.capacity)),
      filterExpression = maybeFilterExpr.map(ZIOAwsConditionExpression(_)),
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap).map(m =>
        m.map { case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v) }
      ),
      keyConditionExpression = maybeKeyExpr.map(ZIOAwsKeyExpression(_))
    )
  }

  private def awsQueryRequest(querySome: QuerySome): QueryRequest = {
    val (aliasMap, (maybeFilterExpr, maybeKeyExpr)) = (for {
      filter  <- AliasMapRender.collectAll(querySome.filterExpression.map(_.render))
      keyExpr <- AliasMapRender.collectAll(querySome.keyConditionExpression.map(_.render))
    } yield (filter, keyExpr)).execute
    QueryRequest(
      tableName = ZIOAwsTableName(querySome.tableName.value),
      indexName = querySome.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = querySome.select.map(awsSelect),
      limit = Some(querySome.limit).map(PositiveIntegerObject(_)),
      consistentRead = Some(toBoolean(querySome.consistency)).map(ZIOAwsConsistentRead(_)),
      scanIndexForward = Some(querySome.ascending),
      exclusiveStartKey = querySome.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      returnConsumedCapacity = Some(awsConsumedCapacity(querySome.capacity)),
      projectionExpression =
        toOption(querySome.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      filterExpression = maybeFilterExpr.map(ZIOAwsConditionExpression(_)),
      expressionAttributeValues = aliasMapToExpressionZIOAwsAttributeValues(aliasMap).map(_.map {
        case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v)
      }),
      keyConditionExpression = maybeKeyExpr.map(ZIOAwsKeyExpression(_))
    )
  }
  private def awsScanRequest(scanAll: ScanAll, segment: Option[ScanAll.Segment]): ScanRequest = {
    val filterExpression = scanAll.filterExpression.map(fe => fe.render.execute)
    ScanRequest(
      tableName = ZIOAwsTableName(scanAll.tableName.value),
      indexName = scanAll.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = scanAll.select.map(awsSelect),
      exclusiveStartKey = scanAll.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      returnConsumedCapacity = Some(awsConsumedCapacity(scanAll.capacity)),
      limit = scanAll.limit.map(PositiveIntegerObject(_)),
      projectionExpression =
        toOption(scanAll.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      filterExpression = filterExpression.map(_._2).map(ZIOAwsConditionExpression(_)),
      expressionAttributeValues = filterExpression
        .flatMap(a => aliasMapToExpressionZIOAwsAttributeValues(a._1))
        .map(_.map { case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v) }),
      consistentRead = Some(toBoolean(scanAll.consistency)).map(ZIOAwsConsistentRead(_)),
      totalSegments = segment.map(_.total).map(ScanTotalSegments(_)),
      segment = segment.map(_.number).map(ScanSegment(_))
    )
  }

  private def awsScanRequest(scanSome: ScanSome): ScanRequest = {
    val filterExpression = scanSome.filterExpression.map(fe => fe.render.execute)
    ScanRequest(
      tableName = ZIOAwsTableName(scanSome.tableName.value),
      indexName = scanSome.indexName.map(_.value).map(ZIOAwsIndexName(_)),
      select = scanSome.select.map(awsSelect),
      exclusiveStartKey = scanSome.exclusiveStartKey.map(m => awsAttributeValueMap(m.map)),
      returnConsumedCapacity = Some(awsConsumedCapacity(scanSome.capacity)),
      limit = Some(scanSome.limit).map(PositiveIntegerObject(_)),
      projectionExpression =
        toOption(scanSome.projections).map(awsProjectionExpression).map(ZIOAwsProjectionExpression(_)),
      filterExpression = filterExpression.map(_._2).map(ZIOAwsConditionExpression(_)),
      expressionAttributeValues = filterExpression
        .flatMap(a => aliasMapToExpressionZIOAwsAttributeValues(a._1))
        .map(_.map { case (k, v) => (ZIOAwsExpressionAttributeValueVariable(k), v) }),
      consistentRead = Some(toBoolean(scanSome.consistency)).map(ZIOAwsConsistentRead(_))
    )
  }

  private[dynamodb] def awsProjectionExpression(
    projectionExpressions: Iterable[ProjectionExpression]
  ) =
    projectionExpressions.mkString(", ")

  private def awsAttributeValueMap(
    attrMap: ScalaMap[String, AttributeValue]
  ): ScalaMap[ZIOAwsAttributeName, ZIOAwsAttributeValue]                                                 =
    attrMap.map { case (k, v) => (ZIOAwsAttributeName(k), awsAttributeValue(v)) }

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
  ): ZIOAwsAttributeValue =
    attributeVal match {
      case AttributeValue.Binary(value)    =>
        ZIOAwsAttributeValue(b = Some(Chunk.fromIterable(value)).map(BinaryAttributeValue(_)))
      case AttributeValue.BinarySet(value) =>
        ZIOAwsAttributeValue(bs = Some(value.map(Chunk.fromIterable).map(BinaryAttributeValue(_))))
      case AttributeValue.Bool(value)      => ZIOAwsAttributeValue(bool = Some(value).map(BooleanAttributeValue(_)))
      case AttributeValue.List(value)      => ZIOAwsAttributeValue(l = Some(value.map(awsAttributeValue)))
      case AttributeValue.Map(value)       =>
        ZIOAwsAttributeValue(m = Some(value.map {
          case (k, v) => (ZIOAwsAttributeName(k.value), awsAttributeValue(v))
        }))
      case AttributeValue.Number(value)    => ZIOAwsAttributeValue(n = Some(value.toString()).map(NumberAttributeValue(_)))
      case AttributeValue.NumberSet(value) =>
        ZIOAwsAttributeValue(ns = Some(value.map(_.toString()).map(NumberAttributeValue(_))))
      case AttributeValue.Null             => ZIOAwsAttributeValue(nul = Some(true).map(NullAttributeValue(_)))
      case AttributeValue.String(value)    => ZIOAwsAttributeValue(s = Some(value).map(StringAttributeValue(_)))
      case AttributeValue.StringSet(value) => ZIOAwsAttributeValue(ss = Some(value.map(StringAttributeValue(_))))
    }

  private[dynamodb] def awsWriteRequest(write: BatchWriteItem.Write): WriteRequest =
    write match {
      case BatchWriteItem.Delete(key) =>
        WriteRequest(
          None,
          Some(DeleteRequest(key.map.map { case (k, v) => (ZIOAwsAttributeName(k), awsAttributeValue(v)) }))
        )
      case BatchWriteItem.Put(item)   =>
        WriteRequest(
          Some(PutRequest(item.map.map { case (k, v) => (ZIOAwsAttributeName(k), awsAttributeValue(v)) })),
          None
        )
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
