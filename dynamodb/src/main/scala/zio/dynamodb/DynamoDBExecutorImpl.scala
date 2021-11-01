package zio.dynamodb
import zio.{ Chunk, ZIO }
import zio.dynamodb.DynamoDBQuery._
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.model.{
  BatchGetItemRequest,
  BatchWriteItemRequest,
  CreateTableRequest,
  DeleteItemRequest,
  DeleteRequest,
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
  Select => ZIOAwsSelect
}
import zio.dynamodb.ConsistencyMode.toBoolean
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.SSESpecification.SSEType
import zio.stream.{ Stream, ZSink }

import scala.collection.immutable.{ Map => ScalaMap }

private[dynamodb] final case class DynamoDBExecutorImpl private (dynamoDb: DynamoDb.Service) extends DynamoDBExecutor {

  def executeMap[A, B](map: Map[A, B]): ZIO[Any, Throwable, B] =
    execute(map.query).map(map.mapper)

  def executeZip[A, B, C](zip: Zip[A, B, C]): ZIO[Any, Throwable, C] =
    execute(zip.left).zipWith(execute(zip.right))(zip.zippable.zip)

  def executeConstructor[A](constructor: Constructor[A]): ZIO[Any, Throwable, A] = {
    println("============= EXECUTING CONSTRUCTOR")
    constructor match {
      case getItem: GetItem               =>
        println("======= GET ITEM")
        doGetItem(getItem)
      case putItem: PutItem               =>
        println("======= PUT ITEM")
        doPutItem(putItem)
      case batchGetItem: BatchGetItem     =>
        println("====== BATCH GET ITEM")
        doBatchGetItem(batchGetItem)
      case batchWriteItem: BatchWriteItem =>
        println("======= BATCH WRITE ITEM")
        doBatchWriteItem(batchWriteItem)
      case scanAll: ScanAll               =>
        println("========= SCAN ALL")
        doScanAll(scanAll)
      case scanSome: ScanSome             =>
        println("======= SCAN SOME")
        doScanSome(scanSome)
      case updateItem: UpdateItem         =>
        println("===== UPDATE ITEM")
        doUpdateItem(updateItem)
      case createTable: CreateTable       =>
        println("====== CREATE TABLE")
        doCreateTable(createTable)
      case deleteItem: DeleteItem         =>
        println("======== DELETE ITEM")
        doDeleteItem(deleteItem)
      case querySome: QuerySome           =>
        println("====== QUERY SOME")
        doQuerySome(querySome)
      case queryAll: QueryAll             =>
        println("======== QUERY ALL")
        doQueryAll(queryAll)
      case Succeed(thunk)                 =>
        println("========= THUNK")
        ZIO.succeed(thunk())
    }
  }

  override def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Throwable, A] =
    atomicQuery match {
      case constructor: Constructor[_] => executeConstructor(constructor)
      case zip @ Zip(_, _, _)          => executeZip(zip)
      case map @ Map(_, _)             => executeMap(map)
    }

  private def doCreateTable(createTable: CreateTable): ZIO[Any, Throwable, Unit] =
    dynamoDb.createTable(generateCreateTableRequest(createTable)).mapError(_.toThrowable).unit

  private def doDeleteItem(deleteItem: DeleteItem): ZIO[Any, Throwable, Unit] =
    dynamoDb.deleteItem(generateDeleteItemRequest(deleteItem)).mapError(_.toThrowable).unit

  private def generateDeleteItemRequest(deleteItem: DeleteItem): DeleteItemRequest =
    DeleteItemRequest(
      tableName = deleteItem.tableName.value,
      key = deleteItem.key.map.map { case (k, v) => (k, buildAwsAttributeValue(v)) },
      conditionExpression = deleteItem.conditionExpression.map(_.toString),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(deleteItem.capacity)),
      returnItemCollectionMetrics = Some(buildAwsItemMetrics(deleteItem.itemMetrics)),
      returnValues = Some(buildAwsReturnValue(deleteItem.returnValues))
    )

  private def doQuerySome(querySome: QuerySome): ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = {
    val stream =
      dynamoDb
        .query(generateQueryRequest(querySome))
        .mapBoth(_.toThrowable, toDynamoItem)
    val sink   = ZSink.collectAll[Item]
    stream.run(sink).map(chunk => (chunk, Some(chunk.last)))
  }

  private def doQueryAll(queryAll: QueryAll): ZIO[Any, Throwable, Stream[Throwable, Item]] =
    ZIO.succeed(
      dynamoDb
        .query(generateQueryRequest(queryAll))
        .mapBoth(
          awsErr => awsErr.toThrowable,
          item => toDynamoItem(item)
        )
    )

  private def generateQueryRequest(queryAll: QueryAll): QueryRequest =
    QueryRequest(
      tableName = queryAll.tableName.value,
      indexName = queryAll.indexName.map(_.value),
      select = queryAll.select.map(buildAwsSelect),
      limit = queryAll.limit,
      consistentRead = Some(toBoolean(queryAll.consistency)),
      scanIndexForward = Some(queryAll.ascending),
      exclusiveStartKey = queryAll.exclusiveStartKey.map(m => attrMapToAwsAttrMap(m.map)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(queryAll.capacity)),
      projectionExpression = toOption(queryAll.projections).map(_.mkString(", ")),
      filterExpression = queryAll.filterExpression.map(filterExpression => filterExpression.render()),
      keyConditionExpression = queryAll.keyConditionExpression.map(_.render())
    )

  private def generateQueryRequest(querySome: QuerySome): QueryRequest =
    QueryRequest(
      tableName = querySome.tableName.value,
      indexName = querySome.indexName.map(_.value),
      select = querySome.select.map(buildAwsSelect),
      limit = Some(querySome.limit),
      consistentRead = Some(toBoolean(querySome.consistency)),
      scanIndexForward = Some(querySome.ascending),
      exclusiveStartKey = querySome.exclusiveStartKey.map(m => attrMapToAwsAttrMap(m.map)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(querySome.capacity)),
      projectionExpression = toOption(querySome.projections).map(_.mkString(", ")),
      filterExpression = querySome.filterExpression.map(filterExpression => filterExpression.render()),
      keyConditionExpression = querySome.keyConditionExpression.map(_.render())
    )

  private def generateCreateTableRequest(createTable: CreateTable): CreateTableRequest =
    CreateTableRequest(
      attributeDefinitions = createTable.attributeDefinitions.map(buildAwsAttributeDefinition),
      tableName = createTable.tableName.value,
      keySchema = buildAwsKeySchema(createTable.keySchema),
      localSecondaryIndexes = toOption(createTable.localSecondaryIndexes.map(buildAwsLocalSecondaryIndex)),
      globalSecondaryIndexes = toOption(createTable.globalSecondaryIndexes.map(buildAwsGlobalSecondaryIndex)),
      billingMode = Some(buildAwsBillingMode(createTable.billingMode)),
      provisionedThroughput = createTable.billingMode match {
        case BillingMode.Provisioned(pt) =>
          Some(
            ZIOAwsProvisionedThroughput(
              readCapacityUnits = pt.readCapacityUnit,
              writeCapacityUnits = pt.writeCapacityUnit
            )
          )
        case BillingMode.PayPerRequest   => None
      },
      // looks like we don't support stream specs?
      // streamSpecification = ???,
      sseSpecification = createTable.sseSpecification.map(buildAwsSSESpecification),
      tags = Some(createTable.tags.map { case (k, v) => Tag(k, v) })
    )

  private def doBatchWriteItem(batchWriteItem: BatchWriteItem): ZIO[Any, Throwable, Unit] = {
    println(s"========== BATCH WRITE ITEM: $batchWriteItem")
    dynamoDb
      .batchWriteItem(generateBatchWriteItem(batchWriteItem))
      .mapError(_.toThrowable)
      .unit
      .unless(batchWriteItem.requestItems.isEmpty)
  }

  private def generateBatchWriteItem(batchWriteItem: BatchWriteItem): BatchWriteItemRequest = {
    val reqItems = batchWriteItem.requestItems.map {
      case (tableName, items) =>
        (tableName.value, items.map(batchItemWriteToZIOAwsWriteRequest))
    }.toMap

    println(s"====== requestItems: $reqItems")
    BatchWriteItemRequest(
      requestItems = reqItems, // TODO(adam): MapOfSet uses iterable, maybe we should add a mapKeyValues?
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(batchWriteItem.capacity)),
      returnItemCollectionMetrics = Some(buildAwsItemMetrics(batchWriteItem.itemMetrics))
    )
  }

  private def batchItemWriteToZIOAwsWriteRequest(write: BatchWriteItem.Write): WriteRequest =
    write match {
      case BatchWriteItem.Delete(key) =>
        WriteRequest(None, Some(DeleteRequest(key.map.map { case (k, v) => (k, buildAwsAttributeValue(v)) })))
      case BatchWriteItem.Put(item)   =>
        WriteRequest(Some(PutRequest(item.map.map { case (k, v) => (k, buildAwsAttributeValue(v)) })), None)
    }

  private def doUpdateItem(updateItem: UpdateItem): ZIO[Any, Throwable, Unit] =
    dynamoDb.updateItem(generateUpdateItemRequest(updateItem)).mapError(_.toThrowable).unit

  private def doScanSome(scanSome: ScanSome): ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = {
    val stream =
      dynamoDb
        .scan(generateScanRequest(scanSome))
        .mapBoth(_.toThrowable, toDynamoItem)
    val sink   = ZSink.collectAll[Item]
    stream.run(sink).map(chunk => (chunk, Some(chunk.last)))
  }

  private def generateUpdateItemRequest(updateItem: UpdateItem): UpdateItemRequest =
    UpdateItemRequest(
      tableName = updateItem.tableName.value,
      key = updateItem.key.map.map { case (k, v) => (k, buildAwsAttributeValue(v)) },
      returnValues = Some(buildAwsReturnValue(updateItem.returnValues)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(updateItem.capacity)),
      returnItemCollectionMetrics = Some(buildAwsItemMetrics(updateItem.itemMetrics)),
      updateExpression = Some(updateItem.updateExpression.render()),
      conditionExpression = updateItem.conditionExpression.map(_.toString)
    )

  private def doScanAll(scanAll: ScanAll): ZIO[Any, Throwable, Stream[Throwable, Item]] =
    ZIO.succeed(
      dynamoDb
        .scan(generateScanRequest(scanAll))
        .mapBoth(
          awsError => awsError.toThrowable,
          item => toDynamoItem(item)
        )
    )

  private def generateScanRequest(scanAll: ScanSome): ScanRequest =
    ScanRequest(
      tableName = scanAll.tableName.value,
      indexName = scanAll.indexName.map(_.value),
      select = scanAll.select.map(buildAwsSelect),
      exclusiveStartKey = scanAll.exclusiveStartKey.map(m => attrMapToAwsAttrMap(m.map)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(scanAll.capacity)),
      // Looks like we're not currently supporting segments?
      totalSegments = None,
      segment = None,
      limit = Some(scanAll.limit),
      projectionExpression = toOption(scanAll.projections).map(_.mkString(", ")),
      filterExpression = scanAll.filterExpression.map(filterExpression => filterExpression.render()),
      consistentRead = Some(toBoolean(scanAll.consistency))
    )

  private def generateScanRequest(scanAll: ScanAll): ScanRequest =
    ScanRequest(
      tableName = scanAll.tableName.value,
      indexName = scanAll.indexName.map(_.value),
      select = scanAll.select.map(buildAwsSelect),
      exclusiveStartKey = scanAll.exclusiveStartKey.map(m => attrMapToAwsAttrMap(m.map)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(scanAll.capacity)),
      // Looks like we're not currently supporting segments?
      totalSegments = None,
      segment = None,
      limit = scanAll.limit,
      projectionExpression = toOption(scanAll.projections).map(_.mkString(", ")),
      filterExpression = scanAll.filterExpression.map(filterExpression => filterExpression.render()),
      consistentRead = Some(toBoolean(scanAll.consistency))
    )

  private def doBatchGetItem(batchGetItem: BatchGetItem): ZIO[Any, Throwable, BatchGetItem.Response] =
    if (batchGetItem.requestItems.isEmpty) ZIO.succeed(BatchGetItem.Response())
    else
      (for {
        a <- dynamoDb.batchGetItem(generateBatchGetItemRequest(batchGetItem))
        b <- a.responses
      } yield BatchGetItem.Response(
        b.foldLeft(MapOfSet.empty[TableName, Item]) {
          case (acc, (tableName, list)) => acc ++ ((TableName(tableName), list.map(toDynamoItem)))
        }
      )).mapError(_.toThrowable)

  private def doPutItem(putItem: PutItem): ZIO[Any, Throwable, Unit] = {
    println(s"========== PUT ITEM: $putItem")
    dynamoDb.putItem(generatePutItemRequest(putItem)).unit.mapError(_.toThrowable)
  }

  private def doGetItem(getItem: GetItem): ZIO[Any, Throwable, Option[Item]] =
    for {
      a <- dynamoDb
             .getItem(generateGetItemRequest(getItem))
             .mapError(_.toThrowable)
      c  = a.itemValue.map(toDynamoItem)
    } yield c

  private def toDynamoItem(attrMap: ScalaMap[String, ZIOAwsAttributeValue.ReadOnly]): Item =
    Item(attrMap.flatMap { case (k, v) => awsAttrValToAttrVal(v).map(attrVal => (k, attrVal)) })

  private def generateBatchGetItemRequest(batchGetItem: BatchGetItem): BatchGetItemRequest = {
    val a = batchGetItem.requestItems.map {
      case (tableName, tableGet) =>
        (tableName.value, generateKeysAndAttributes(tableGet))
    }.toMap
    println(s"====== BATCH GET REQUEST ITEMS: $a")

    BatchGetItemRequest(
      requestItems = a,
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(batchGetItem.capacity))
    )
  }

  private def generateKeysAndAttributes(tableGet: TableGet): KeysAndAttributes =
    KeysAndAttributes(
      keys = tableGet.keysSet.map(set => // So many maps
        set.map.map {
          case (k, v) =>
            (k, buildAwsAttributeValue(v))
        }
      ),
      projectionExpression = toOption(tableGet.projectionExpressionSet).map(_.mkString(", "))
    )

  private def generatePutItemRequest(putItem: PutItem): PutItemRequest =
    PutItemRequest(
      tableName = putItem.tableName.value,
      item = attrMapToAwsAttrMap(putItem.item.map),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(putItem.capacity)),
      returnItemCollectionMetrics = Some(ReturnItemCollectionMetrics.toZioAws(putItem.itemMetrics)),
      conditionExpression = putItem.conditionExpression.map(_.toString),
      returnValues = Some(buildAwsReturnValue(putItem.returnValues))
    )

  private def attrMapToAwsAttrMap(attrMap: ScalaMap[String, AttributeValue]): ScalaMap[String, ZIOAwsAttributeValue] =
    attrMap.map { case (k, v) => (k, buildAwsAttributeValue(v)) }

  private def generateGetItemRequest(getItem: GetItem): GetItemRequest                                               =
    GetItemRequest(
      tableName = getItem.tableName.value,
      key = getItem.key.map.map { case (k, v) => (k, buildAwsAttributeValue(v)) },
      consistentRead = Some(ConsistencyMode.toBoolean(getItem.consistency)),
      returnConsumedCapacity = Some(buildAwsReturnConsumedCapacity(getItem.capacity)),
      projectionExpression = toOption(getItem.projections).map(
        _.mkString(", ") // TODO(adam): Not sure if this is the best way to combine projection expressions
      )
    )

  //  private def awsAttrValToZioAttrVal(
//    attributeValue: ZIOAwsAttributeValue.ReadOnly
//  ): ZIO[Any, Throwable, AttributeValue] =
//    attributeValue.s
//      .map(AttributeValue.String)
//      .orElse(attributeValue.n.map(n => AttributeValue.Number(BigDecimal(n))))
//      .orElse(attributeValue.b.map(AttributeValue.Binary))
//      .orElse(attributeValue.ss.map(ss => AttributeValue.StringSet(ss.toSet)))
//      .orElse(attributeValue.ns.map(ns => AttributeValue.NumberSet(ns.map(BigDecimal(_)).toSet)))
//      .orElse(attributeValue.bs.map(bs => AttributeValue.BinarySet(bs.toSet)))
//      .orElse(
//        attributeValue.m.map(m =>
//          AttributeValue.Map(
//            m.flatMap { case (k, v) => awsAttrValToAttrVal(v).map(attrVal => (AttributeValue.String(k), attrVal)) }
//          )
//        )
//      )
//      .orElse(attributeValue.l.map(l => AttributeValue.List(l.flatMap(awsAttrValToAttrVal))))
//      .orElse(attributeValue.nul.map(_ => AttributeValue.Null))
//      .orElse(attributeValue.bool.map(AttributeValue.Bool))
//      .mapError(_.toThrowable)

  private def awsAttrValToAttrVal(attributeValue: ZIOAwsAttributeValue.ReadOnly): Option[AttributeValue] =
    attributeValue.sValue
      .map(AttributeValue.String)
      .orElse(
        attributeValue.nValue.map(n => AttributeValue.Number(BigDecimal(n)))
      )   // TODO(adam): Does the BigDecimal need a try wrapper?
      .orElse(
        attributeValue.bValue.map(b => AttributeValue.Binary(b))
      )
      .orElse(
        attributeValue.ssValue.map(s =>
          AttributeValue.StringSet(s.toSet)
        ) // TODO(adam): Is this `toSet` actually safe to do?
      )
      .orElse(
        attributeValue.nsValue.map(ns =>
          AttributeValue.NumberSet(ns.map(BigDecimal(_)).toSet)
        ) // TODO(adam): Wrap in try?
      )
      .orElse(
        attributeValue.bsValue.map(bs => AttributeValue.BinarySet(bs.toSet))
      )
      .orElse(
        attributeValue.mValue.map(m =>
          AttributeValue.Map(
            m.flatMap {
              case (k, v) =>
                awsAttrValToAttrVal(v).map(attrVal => (AttributeValue.String(k), attrVal))
            }
          )
        )
      )
      .orElse(attributeValue.lValue.map(l => AttributeValue.List(l.flatMap(awsAttrValToAttrVal))))
      .orElse(attributeValue.nulValue.map(_ => AttributeValue.Null))
      .orElse(attributeValue.boolValue.map(AttributeValue.Bool))

  private def buildAwsItemMetrics(metrics: ReturnItemCollectionMetrics): ZIOAwsReturnItemCollectionMetrics =
    metrics match {
      case ReturnItemCollectionMetrics.None => ZIOAwsReturnItemCollectionMetrics.NONE
      case ReturnItemCollectionMetrics.Size => ZIOAwsReturnItemCollectionMetrics.SIZE
    }

  private def buildAwsReturnConsumedCapacity(
    returnConsumedCapacity: ReturnConsumedCapacity
  ): ZIOAwsReturnConsumedCapacity =
    returnConsumedCapacity match {
      case ReturnConsumedCapacity.Indexes => ZIOAwsReturnConsumedCapacity.INDEXES
      case ReturnConsumedCapacity.Total   => ZIOAwsReturnConsumedCapacity.TOTAL
      case ReturnConsumedCapacity.None    => ZIOAwsReturnConsumedCapacity.NONE
    }

  private def toOption[A](list: List[A]): Option[::[A]] =
    list match {
      case Nil          => None
      case head :: tail => Some(::(head, tail))
    }

  private def toOption[A](set: Set[A]): Option[Set[A]] =
    if (set.isEmpty) None else Some(set)

  private def buildAwsReturnValue(
    returnValues: ReturnValues
  ): ReturnValue =
    returnValues match {
      case ReturnValues.None       => ReturnValue.NONE
      case ReturnValues.AllOld     => ReturnValue.ALL_OLD
      case ReturnValues.UpdatedOld => ReturnValue.UPDATED_OLD
      case ReturnValues.AllNew     => ReturnValue.ALL_NEW
      case ReturnValues.UpdatedNew => ReturnValue.UPDATED_NEW
    }

  private def buildAwsSelect(
    select: Select
  ): ZIOAwsSelect =
    select match {
      case Select.AllAttributes          => ZIOAwsSelect.ALL_ATTRIBUTES
      case Select.AllProjectedAttributes => ZIOAwsSelect.ALL_PROJECTED_ATTRIBUTES
      case Select.SpecificAttributes     => ZIOAwsSelect.SPECIFIC_ATTRIBUTES
      case Select.Count                  => ZIOAwsSelect.COUNT
    }

  private def buildAwsBillingMode(
    billingMode: BillingMode
  ): ZIOAwsBillingMode =
    billingMode match {
      case BillingMode.Provisioned(_) => ZIOAwsBillingMode.PROVISIONED
      case BillingMode.PayPerRequest  => ZIOAwsBillingMode.PAY_PER_REQUEST
    }

  private def buildAwsAttributeDefinition(attributeDefinition: AttributeDefinition): ZIOAwsAttributeDefinition =
    ZIOAwsAttributeDefinition(
      attributeName = attributeDefinition.name,
      attributeType = attributeDefinition.attributeType match {
        case AttributeValueType.Binary => ScalarAttributeType.B
        case AttributeValueType.Number => ScalarAttributeType.N
        case AttributeValueType.String => ScalarAttributeType.S
      }
    )

  private def buildAwsGlobalSecondaryIndex(globalSecondaryIndex: GlobalSecondaryIndex): ZIOAwsGlobalSecondaryIndex =
    ZIOAwsGlobalSecondaryIndex(
      indexName = globalSecondaryIndex.indexName,
      keySchema = buildAwsKeySchema(globalSecondaryIndex.keySchema),
      projection = buildAwsProjectionType(globalSecondaryIndex.projection),
      provisionedThroughput = globalSecondaryIndex.provisionedThroughput.map(provisionedThroughput =>
        ZIOAwsProvisionedThroughput(
          readCapacityUnits = provisionedThroughput.readCapacityUnit,
          writeCapacityUnits = provisionedThroughput.writeCapacityUnit
        )
      )
    )

  private def buildAwsLocalSecondaryIndex(localSecondaryIndex: LocalSecondaryIndex): ZIOAwsLocalSecondaryIndex =
    ZIOAwsLocalSecondaryIndex(
      indexName = localSecondaryIndex.indexName,
      keySchema = buildAwsKeySchema(localSecondaryIndex.keySchema),
      projection = buildAwsProjectionType(localSecondaryIndex.projection)
    )

  private def buildAwsProjectionType(projectionType: ProjectionType): ZIOAwsProjection =
    projectionType match {
      case ProjectionType.KeysOnly                  =>
        ZIOAwsProjection(projectionType = Some(ZIOAwsProjectionType.KEYS_ONLY), nonKeyAttributes = None)
      case ProjectionType.Include(nonKeyAttributes) =>
        ZIOAwsProjection(projectionType = Some(ZIOAwsProjectionType.INCLUDE), nonKeyAttributes = Some(nonKeyAttributes))
      case ProjectionType.All                       => ZIOAwsProjection(projectionType = Some(ZIOAwsProjectionType.ALL), None)
    }

  private def buildAwsSSESpecification(sseSpecification: SSESpecification): ZIOAwsSSESpecification =
    ZIOAwsSSESpecification(
      enabled = Some(sseSpecification.enable),
      sseType = Some(buildAwsSSEType(sseSpecification.sseType)),
      kmsMasterKeyId = sseSpecification.kmsMasterKeyId
    )

  private def buildAwsSSEType(sseType: SSEType): ZIOAwsSSEType =
    sseType match {
      case SSESpecification.AES256 => ZIOAwsSSEType.AES256
      case SSESpecification.KMS    => ZIOAwsSSEType.KMS
    }

  private def buildAwsKeySchema(keySchema: KeySchema): List[KeySchemaElement] = {
    val hashKeyElement = List(
      KeySchemaElement(
        attributeName = keySchema.hashKey,
        keyType = KeyType.HASH
      )
    )
    keySchema.sortKey.fold(hashKeyElement)(sortKey =>
      hashKeyElement.appended(KeySchemaElement(attributeName = sortKey, keyType = KeyType.RANGE))
    )
  }

  private def buildAwsAttributeValue(
    attributeVal: AttributeValue
  ): ZIOAwsAttributeValue =
    attributeVal match {
      case AttributeValue.Binary(value)    => ZIOAwsAttributeValue(b = Some(Chunk.fromIterable(value)))
      case AttributeValue.BinarySet(value) => ZIOAwsAttributeValue(bs = Some(value.map(Chunk.fromIterable)))
      case AttributeValue.Bool(value)      => ZIOAwsAttributeValue(bool = Some(value))
      case AttributeValue.List(value)      => ZIOAwsAttributeValue(l = Some(value.map(buildAwsAttributeValue)))
      case AttributeValue.Map(value)       =>
        ZIOAwsAttributeValue(m = Some(value.map {
          case (k, v) => (k.value, buildAwsAttributeValue(v))
        }.toMap)) // TODO(adam): Why does this require a toMap?
      case AttributeValue.Number(value)    => ZIOAwsAttributeValue(n = Some(value.toString()))
      case AttributeValue.NumberSet(value) => ZIOAwsAttributeValue(ns = Some(value.map(_.toString())))
      case AttributeValue.Null             => ZIOAwsAttributeValue(nul = Some(true))
      case AttributeValue.String(value)    => ZIOAwsAttributeValue(s = Some(value))
      case AttributeValue.StringSet(value) => ZIOAwsAttributeValue(ss = Some(value))
    }

}
