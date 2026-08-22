/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition => AwsAttributeDefinition,
  AttributeValue => AwsAttrValue,
  BatchGetItemRequest,
  BatchGetItemResponse,
  BatchWriteItemRequest,
  BatchWriteItemResponse,
  BillingMode => AwsBillingMode,
  CancellationReason => AwsCancellationReason,
  Capacity => AwsCapacity,
  ConditionCheck => AwsConditionCheck,
  ConsumedCapacity => AwsConsumedCapacity,
  CreateTableRequest,
  CreateTableResponse,
  Delete => AwsTransactDelete,
  DeleteItemRequest,
  DeleteItemResponse,
  DeleteRequest,
  DeleteTableRequest,
  DeleteTableResponse,
  DescribeTableRequest,
  DescribeTableResponse => AwsDescribeTableResponse,
  Get => AwsTransactGet,
  GetItemRequest,
  GetItemResponse,
  GlobalSecondaryIndex => AwsGlobalSecondaryIndex,
  ItemCollectionMetrics => AwsItemCollectionMetrics,
  ItemResponse,
  KeySchemaElement,
  KeyType,
  KeysAndAttributes,
  LocalSecondaryIndex => AwsLocalSecondaryIndex,
  Projection => AwsProjection,
  ProjectionType => AwsProjectionType,
  ProvisionedThroughput => AwsProvisionedThroughput,
  Put => AwsTransactPut,
  PutItemRequest,
  PutItemResponse,
  PutRequest,
  QueryRequest,
  QueryResponse,
  ReturnConsumedCapacity => AwsReturnConsumedCapacity,
  ReturnItemCollectionMetrics => AwsReturnItemCollectionMetrics,
  ReturnValue => AwsReturnValue,
  ReturnValuesOnConditionCheckFailure => AwsReturnValuesOnConditionCheckFailure,
  SSESpecification => AwsSseSpecification,
  SSEType => AwsSseType,
  ScalarAttributeType,
  ScanRequest,
  ScanResponse,
  Select => AwsSelect,
  TableStatus => AwsTableStatus,
  Tag,
  TransactGetItem,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItem => AwsTransactWriteItem,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  TransactionCanceledException,
  Update => AwsTransactUpdate,
  UpdateItemRequest,
  UpdateItemResponse,
  WriteRequest
}
import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError

import java.util.{ HashMap => JHashMap, Map => JMap }
import scala.collection.JavaConverters._

// -- Thin I/O interface --------------------------------------------------

/**
 * A thin wrapper over the AWS SDK's own request/response types — one method per DynamoDB
 * operation, with the raw SDK request in and the raw SDK response out. The only thing that
 * varies between concrete implementations is the return type `F[_]`; `ZioInterpreter`,
 * `CEInterpreter`, and `FutureInterpreter` each supply one built directly from
 * `DynamoDbAsyncClient`. [[InterceptingAwsDynamoDB]] wraps an instance of this trait to add
 * `ResponseInterceptor` metadata reporting without touching the per-operation methods
 * themselves.
 */
trait AwsDynamoDB[F[_]] {
  def getItem(req: GetItemRequest): F[GetItemResponse]
  def putItem(req: PutItemRequest): F[PutItemResponse]
  def updateItem(req: UpdateItemRequest): F[UpdateItemResponse]
  def deleteItem(req: DeleteItemRequest): F[DeleteItemResponse]
  def batchGetItem(req: BatchGetItemRequest): F[BatchGetItemResponse]
  def batchWriteItem(req: BatchWriteItemRequest): F[BatchWriteItemResponse]
  def query(req: QueryRequest): F[QueryResponse]
  def scan(req: ScanRequest): F[ScanResponse]
  def createTable(req: CreateTableRequest): F[CreateTableResponse]
  def deleteTable(req: DeleteTableRequest): F[DeleteTableResponse]
  def describeTable(req: DescribeTableRequest): F[AwsDescribeTableResponse]
  def transactGetItems(req: TransactGetItemsRequest): F[TransactGetItemsResponse]
  def transactWriteItems(req: TransactWriteItemsRequest): F[TransactWriteItemsResponse]
}

// -- Pure codecs ---------------------------------------------------------
// Converts zio-dynamodb ADT ↔ AWS SDK types.
// No effects — safe to share across all interpreters.

private[dynamodb] object AwsCodecs {

  // AttributeValue: domain → AWS
  // Note: AttributeValue.Map keys are AttributeValue.String (not Predef.String),
  // because AttributeValue's companion shadows Predef.String.
  def toAwsAttrValue(av: AttributeValue): Option[AwsAttrValue] =
    av match {
      case AttributeValue.String(v)                  => Some(AwsAttrValue.builder().s(v).build())
      case AttributeValue.Number(v)                  => Some(AwsAttrValue.builder().n(v.toString).build())
      case AttributeValue.Bool(v)                    => Some(AwsAttrValue.builder().bool(v).build())
      case AttributeValue.Null                       => Some(AwsAttrValue.builder().nul(true).build())
      case AttributeValue.Binary(v)                  => Some(AwsAttrValue.builder().b(SdkBytes.fromByteArray(v)).build())
      case AttributeValue.BinarySet(v) if v.nonEmpty =>
        Some(AwsAttrValue.builder().bs(v.map(bytes => SdkBytes.fromByteArray(bytes.toArray)).toList.asJava).build())
      case AttributeValue.StringSet(v) if v.nonEmpty =>
        Some(AwsAttrValue.builder().ss(v.asJava).build())
      case AttributeValue.NumberSet(v) if v.nonEmpty =>
        Some(AwsAttrValue.builder().ns(v.map(_.toString).asJava).build())
      case AttributeValue.List(v)                    =>
        Some(AwsAttrValue.builder().l(v.flatMap(toAwsAttrValue).toList.asJava).build())
      case AttributeValue.Map(v)                     =>
        // v: ScalaMap[AttributeValue.String, AttributeValue] — use .value to get Predef.String key
        Some(
          AwsAttrValue
            .builder()
            .m(
              v.flatMap { case (k, v2) => toAwsAttrValue(v2).map(k.value -> _) }.asJava
            )
            .build()
        )
      case _                                         => None // empty sets
    }

  // AttributeValue: AWS → domain
  def fromAwsAttrValue(av: AwsAttrValue): Option[AttributeValue] =
    if (av.s != null) Some(AttributeValue.String(av.s))
    else if (av.n != null) Some(AttributeValue.Number(BigDecimal(av.n)))
    else if (av.bool != null) Some(AttributeValue.Bool(av.bool))
    else if (av.nul != null && av.nul) Some(AttributeValue.Null)
    else if (av.b != null) Some(AttributeValue.Binary(av.b.asByteArray))
    else if (av.hasBs)
      Some(
        AttributeValue.BinarySet(
          av.bs.asScala.map(b => scala.collection.immutable.ArraySeq.unsafeWrapArray(b.asByteArray))
        )
      )
    else if (av.hasSs) Some(AttributeValue.StringSet(av.ss.asScala.toSet))
    else if (av.hasNs) Some(AttributeValue.NumberSet(av.ns.asScala.map(BigDecimal(_)).toSet))
    else if (av.hasL) Some(AttributeValue.List(av.l.asScala.flatMap(fromAwsAttrValue)))
    else if (av.hasM)
      // Keys come as Predef.String; wrap in AttributeValue.String for the Map key type
      Some(
        AttributeValue.Map(
          av.m.asScala.flatMap { case (k, v) => fromAwsAttrValue(v).map(AttributeValue.String(k) -> _) }.toMap
        )
      )
    else None

  // AttrMap ↔ AWS item map (Predef.String keys)
  def toAwsItem(attrMap: AttrMap): JMap[String, AwsAttrValue] = {
    val result = new JHashMap[String, AwsAttrValue](attrMap.map.size)
    attrMap.foreachEntry((k, v) => toAwsAttrValue(v).foreach(result.put(k, _)))
    result
  }

  def fromAwsItem(m: JMap[String, AwsAttrValue]): Item = {
    val jm = new JHashMap[String, AttributeValue](m.size)
    val it = m.entrySet().iterator()
    while (it.hasNext) {
      val e = it.next()
      fromAwsAttrValue(e.getValue).foreach(jm.put(e.getKey, _))
    }
    AttrMap.fromJavaMap(jm)
  }

  // GetItem
  def toGetItemRequest(getItem: DynamoDBQuery.GetItem): GetItemRequest = {
    val (aliasMap, projections: List[String]) =
      AliasMapRender.forEach(getItem.projections.toList).execute
    val names                                 = aliasMap.map.iterator.collect { case (AliasMap.PathSegment(_, segment), alias) =>
      alias -> segment
    }.toMap
    val b                                     = GetItemRequest
      .builder()
      .tableName(getItem.tableName)
      .key(toAwsItem(getItem.key))
      .consistentRead(getItem.consistency == ConsistencyMode.Strong)
    if (projections.nonEmpty) b.projectionExpression(projections.mkString(", "))
    if (names.nonEmpty) b.expressionAttributeNames(names.asJava)
    b.returnConsumedCapacity(toAwsReturnConsumedCapacity(getItem.capacity))
    b.build()
  }

  def fromGetItemResponse(r: GetItemResponse): Option[Item] =
    if (r.hasItem && !r.item.isEmpty) Some(fromAwsItem(r.item)) else None

  // Renders a ConditionExpression (or FilterExpression, which is the same type) to the
  // triple that AWS SDK request builders expect: expression string, attribute name aliases,
  // attribute value aliases.  Either map may be empty (e.g. attribute_exists has no value).
  private def renderExpression(
    ce: ConditionExpression[_]
  ): (String, Map[String, String], Map[String, AwsAttrValue]) = {
    val (aliasMap, exprStr) = ce.render.execute
    val names               = aliasMap.map.iterator.collect { case (AliasMap.PathSegment(_, segment), alias) =>
      alias -> segment
    }.toMap
    val values              = aliasMap.map.iterator.collect { case (AliasMap.AttributeValueKey(av), alias) =>
      toAwsAttrValue(av).map(alias -> _)
    }.flatten.toMap
    (exprStr, names, values)
  }

  // PutItem
  def toPutItemRequest(q: DynamoDBQuery.PutItem): PutItemRequest = {
    val b = PutItemRequest
      .builder()
      .tableName(q.tableName)
      .item(toAwsItem(q.item))
    q.conditionExpression.foreach { ce =>
      val (exprStr, names, values) = renderExpression(ce)
      b.conditionExpression(exprStr)
      if (names.nonEmpty) b.expressionAttributeNames(names.asJava)
      if (values.nonEmpty) b.expressionAttributeValues(values.asJava)
    }
    b.returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
    b.returnValues(toAwsReturnValue(q.returnValues))
    b.returnItemCollectionMetrics(toAwsReturnItemCollectionMetrics(q.itemMetrics))
    b.build()
  }

  def fromPutItemResponse(r: PutItemResponse): Option[Item] =
    if (r.hasAttributes && !r.attributes.isEmpty) Some(fromAwsItem(r.attributes)) else None

  // UpdateItem
  def toUpdateItemRequest(q: DynamoDBQuery.UpdateItem): UpdateItemRequest = {
    val composed                            = for {
      updateStr <- q.updateExpression.render
      condStr   <- AliasMapRender.collectAll(q.conditionExpression.map(_.render))
    } yield (updateStr, condStr)
    val (aliasMap, (updateStr, condStrOpt)) = composed.execute
    val names                               = aliasMap.map.iterator.collect { case (AliasMap.PathSegment(_, segment), alias) =>
      alias -> segment
    }.toMap
    val values                              = aliasMap.map.iterator.collect { case (AliasMap.AttributeValueKey(av), alias) =>
      toAwsAttrValue(av).map(alias -> _)
    }.flatten.toMap
    val b                                   = UpdateItemRequest
      .builder()
      .tableName(q.tableName)
      .key(toAwsItem(q.key))
      .updateExpression(updateStr)
    condStrOpt.foreach(b.conditionExpression(_))
    if (names.nonEmpty) b.expressionAttributeNames(names.asJava)
    if (values.nonEmpty) b.expressionAttributeValues(values.asJava)
    b.returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
    b.returnValues(toAwsReturnValue(q.returnValues))
    b.returnItemCollectionMetrics(toAwsReturnItemCollectionMetrics(q.itemMetrics))
    b.build()
  }

  def fromUpdateItemResponse(r: UpdateItemResponse): Option[Item] =
    if (r.hasAttributes && !r.attributes.isEmpty) Some(fromAwsItem(r.attributes)) else None

  // DeleteItem
  def toDeleteItemRequest(q: DynamoDBQuery.DeleteItem): DeleteItemRequest = {
    val b = DeleteItemRequest
      .builder()
      .tableName(q.tableName)
      .key(toAwsItem(q.key))
    q.conditionExpression.foreach { ce =>
      val (exprStr, names, values) = renderExpression(ce)
      b.conditionExpression(exprStr)
      if (names.nonEmpty) b.expressionAttributeNames(names.asJava)
      if (values.nonEmpty) b.expressionAttributeValues(values.asJava)
    }
    b.returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
    b.returnValues(toAwsReturnValue(q.returnValues))
    b.returnItemCollectionMetrics(toAwsReturnItemCollectionMetrics(q.itemMetrics))
    b.build()
  }

  def fromDeleteItemResponse(r: DeleteItemResponse): Option[Item] =
    if (r.hasAttributes && !r.attributes.isEmpty) Some(fromAwsItem(r.attributes)) else None

  // Query
  def toQueryRequest(q: DynamoDBQuery.Query): QueryRequest = {
    val b                                               = QueryRequest
      .builder()
      .tableName(q.tableName)
      .consistentRead(q.consistency == ConsistencyMode.Strong)
      .scanIndexForward(q.ascending)
      .limit(q.limit)
    q.indexName.foreach(b.indexName(_))
    q.exclusiveStartKey.foreach(lek => b.exclusiveStartKey(toAwsItem(lek)))
    val composed                                        = for {
      keyStr    <- AliasMapRender.collectAll(q.keyConditionExpr.map(_.render))
      filterStr <- AliasMapRender.collectAll(q.filterExpression.map(_.render))
      projStrs  <- AliasMapRender.forEach(q.projections)
    } yield (keyStr, filterStr, projStrs)
    val (aliasMap, (keyStrOpt, filterStrOpt, projStrs)) = composed.execute
    val names                                           = aliasMap.map.iterator.collect { case (AliasMap.PathSegment(_, segment), alias) =>
      alias -> segment
    }.toMap
    val values                                          = aliasMap.map.iterator.collect { case (AliasMap.AttributeValueKey(av), alias) =>
      toAwsAttrValue(av).map(alias -> _)
    }.flatten.toMap
    keyStrOpt.foreach(b.keyConditionExpression(_))
    filterStrOpt.foreach(b.filterExpression(_))
    if (projStrs.nonEmpty) b.projectionExpression(projStrs.mkString(", "))
    if (names.nonEmpty) b.expressionAttributeNames(names.asJava)
    if (values.nonEmpty) b.expressionAttributeValues(values.asJava)
    b.returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
    q.select.foreach(s => b.select(toAwsSelect(s)))
    b.build()
  }

  // Scan — shared request shape for Scan
  private def baseScanRequest(
    tableName: String,
    indexName: Option[String],
    consistency: ConsistencyMode,
    limit: Option[Int],
    exclusiveStartKey: LastEvaluatedKey
  ): ScanRequest.Builder = {
    val b = ScanRequest
      .builder()
      .tableName(tableName)
      .consistentRead(consistency == ConsistencyMode.Strong)
    indexName.foreach(b.indexName(_))
    limit.foreach(l => b.limit(l))
    exclusiveStartKey.foreach(lek => b.exclusiveStartKey(toAwsItem(lek)))
    b
  }

  def toScanRequest(q: DynamoDBQuery.Scan): ScanRequest = {
    val b = baseScanRequest(q.tableName, q.indexName, q.consistency, Some(q.limit), q.exclusiveStartKey)
    q.filterExpression.foreach { fe =>
      val (exprStr, names, values) = renderExpression(fe)
      b.filterExpression(exprStr)
      if (names.nonEmpty) b.expressionAttributeNames(names.asJava)
      if (values.nonEmpty) b.expressionAttributeValues(values.asJava)
    }
    b.returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
    q.select.foreach(s => b.select(toAwsSelect(s)))
    if (q.totalSegments > 1) {
      b.segment(q.segment)
      b.totalSegments(q.totalSegments)
    }
    b.build()
  }

  def fromScanResponse(r: ScanResponse): Page[Item] = {
    val items = if (r.hasItems) Chunk.fromIterable(r.items.asScala.map(fromAwsItem)) else Chunk.empty[Item]
    val lek   =
      if (r.hasLastEvaluatedKey && !r.lastEvaluatedKey.isEmpty)
        Some(fromAwsItem(r.lastEvaluatedKey))
      else None
    Page(items, lek, r.count, r.scannedCount)
  }

  def fromQueryResponse(r: QueryResponse): Page[Item] = {
    val items = if (r.hasItems) Chunk.fromIterable(r.items.asScala.map(fromAwsItem)) else Chunk.empty[Item]
    val lek   =
      if (r.hasLastEvaluatedKey && !r.lastEvaluatedKey.isEmpty)
        Some(fromAwsItem(r.lastEvaluatedKey))
      else None
    Page(items, lek, r.count, r.scannedCount)
  }

  // CreateTable helpers
  private def toScalarAttributeType(pvt: PrimitiveValueType): ScalarAttributeType =
    pvt match {
      case AttributeValueType.Binary => ScalarAttributeType.B
      case AttributeValueType.Number => ScalarAttributeType.N
      case AttributeValueType.String => ScalarAttributeType.S
    }

  private def toKeySchemaElements(ks: KeySchema): java.util.List[KeySchemaElement] = {
    val hash = KeySchemaElement.builder().attributeName(ks.hashKey).keyType(KeyType.HASH).build()
    ks.sortKey match {
      case None     => List(hash).asJava
      case Some(sk) => List(hash, KeySchemaElement.builder().attributeName(sk).keyType(KeyType.RANGE).build()).asJava
    }
  }

  private def toAwsProjection(pt: ProjectionType): AwsProjection =
    pt match {
      case ProjectionType.KeysOnly       =>
        AwsProjection.builder().projectionType(AwsProjectionType.KEYS_ONLY).build()
      case ProjectionType.All            =>
        AwsProjection.builder().projectionType(AwsProjectionType.ALL).build()
      case ProjectionType.Include(attrs) =>
        AwsProjection
          .builder()
          .projectionType(AwsProjectionType.INCLUDE)
          .nonKeyAttributes(attrs.toList.asJava)
          .build()
    }

  private def toAwsGsi(gsi: GlobalSecondaryIndex): AwsGlobalSecondaryIndex = {
    val b = AwsGlobalSecondaryIndex
      .builder()
      .indexName(gsi.indexName)
      .keySchema(toKeySchemaElements(gsi.keySchema))
      .projection(toAwsProjection(gsi.projection))
    gsi.provisionedThroughput.foreach(pt =>
      b.provisionedThroughput(
        AwsProvisionedThroughput
          .builder()
          .readCapacityUnits(pt.readCapacityUnit)
          .writeCapacityUnits(pt.writeCapacityUnit)
          .build()
      )
    )
    b.build()
  }

  private def toAwsLsi(lsi: LocalSecondaryIndex): AwsLocalSecondaryIndex =
    AwsLocalSecondaryIndex
      .builder()
      .indexName(lsi.indexName)
      .keySchema(toKeySchemaElements(lsi.keySchema))
      .projection(toAwsProjection(lsi.projection))
      .build()

  private def toAwsSse(sse: SSESpecification): AwsSseSpecification = {
    val b       = AwsSseSpecification.builder().enabled(sse.enable)
    sse.kmsMasterKeyId.foreach(b.kmsMasterKeyId(_))
    val sseType = sse.sseType match {
      case SSESpecification.AES256 => AwsSseType.AES256
      case SSESpecification.KMS    => AwsSseType.KMS
    }
    b.sseType(sseType).build()
  }

  def toCreateTableRequest(q: DynamoDBQuery.CreateTable): CreateTableRequest = {
    val b = CreateTableRequest
      .builder()
      .tableName(q.tableName)
      .keySchema(toKeySchemaElements(q.keySchema))
      .attributeDefinitions(
        q.attributeDefinitions
          .map(ad =>
            AwsAttributeDefinition
              .builder()
              .attributeName(ad.name)
              .attributeType(toScalarAttributeType(ad.attributeType))
              .build()
          )
          .toList
          .asJava
      )
    q.billingMode match {
      case BillingMode.PayPerRequest   =>
        b.billingMode(AwsBillingMode.PAY_PER_REQUEST)
      case BillingMode.Provisioned(pt) =>
        b.billingMode(AwsBillingMode.PROVISIONED)
          .provisionedThroughput(
            AwsProvisionedThroughput
              .builder()
              .readCapacityUnits(pt.readCapacityUnit)
              .writeCapacityUnits(pt.writeCapacityUnit)
              .build()
          )
    }
    if (q.globalSecondaryIndexes.nonEmpty)
      b.globalSecondaryIndexes(q.globalSecondaryIndexes.map(toAwsGsi).toList.asJava)
    if (q.localSecondaryIndexes.nonEmpty)
      b.localSecondaryIndexes(q.localSecondaryIndexes.map(toAwsLsi).toList.asJava)
    q.sseSpecification.foreach(sse => b.sseSpecification(toAwsSse(sse)))
    if (q.tags.nonEmpty)
      b.tags(q.tags.map { case (k, v) => Tag.builder().key(k).value(v).build() }.toList.asJava)
    b.build()
  }

  def toDeleteTableRequest(q: DynamoDBQuery.DeleteTable): DeleteTableRequest =
    DeleteTableRequest.builder().tableName(q.tableName).build()

  def toDescribeTableRequest(q: DynamoDBQuery.DescribeTable): DescribeTableRequest =
    DescribeTableRequest.builder().tableName(q.tableName).build()

  private def toAwsReturnConsumedCapacity(c: ReturnConsumedCapacity): AwsReturnConsumedCapacity =
    c match {
      case ReturnConsumedCapacity.Indexes => AwsReturnConsumedCapacity.INDEXES
      case ReturnConsumedCapacity.Total   => AwsReturnConsumedCapacity.TOTAL
      case ReturnConsumedCapacity.None    => AwsReturnConsumedCapacity.NONE
    }

  private def toAwsReturnValue(rv: ReturnValues): AwsReturnValue =
    rv match {
      case ReturnValues.None       => AwsReturnValue.NONE
      case ReturnValues.AllOld     => AwsReturnValue.ALL_OLD
      case ReturnValues.UpdatedOld => AwsReturnValue.UPDATED_OLD
      case ReturnValues.AllNew     => AwsReturnValue.ALL_NEW
      case ReturnValues.UpdatedNew => AwsReturnValue.UPDATED_NEW
    }

  private def toAwsReturnItemCollectionMetrics(m: ReturnItemCollectionMetrics): AwsReturnItemCollectionMetrics =
    m match {
      case ReturnItemCollectionMetrics.None => AwsReturnItemCollectionMetrics.NONE
      case ReturnItemCollectionMetrics.Size => AwsReturnItemCollectionMetrics.SIZE
    }

  private def toAwsSelect(s: Select): AwsSelect =
    s match {
      case Select.AllAttributes          => AwsSelect.ALL_ATTRIBUTES
      case Select.AllProjectedAttributes => AwsSelect.ALL_PROJECTED_ATTRIBUTES
      case Select.SpecificAttributes     => AwsSelect.SPECIFIC_ATTRIBUTES
      case Select.Count                  => AwsSelect.COUNT
    }

  private def fromAwsCapacity(c: AwsCapacity): zio.dynamodb.Capacity =
    zio.dynamodb.Capacity(
      readCapacityUnits = Option(c.readCapacityUnits(): java.lang.Double).map(_.doubleValue()),
      writeCapacityUnits = Option(c.writeCapacityUnits(): java.lang.Double).map(_.doubleValue()),
      capacityUnits = Option(c.capacityUnits(): java.lang.Double).map(_.doubleValue())
    )

  /**
   * Converts an AWS SDK [[AwsConsumedCapacity]] to the library's [[ConsumedCapacity]],
   *  including per-LSI and per-GSI breakdowns keyed by index name.
   */
  private[dynamodb] def fromAwsConsumedCapacity(c: AwsConsumedCapacity): zio.dynamodb.ConsumedCapacity =
    zio.dynamodb.ConsumedCapacity(
      tableName = Option(c.tableName()),
      readCapacityUnits = Option(c.readCapacityUnits(): java.lang.Double).map(_.doubleValue()),
      writeCapacityUnits = Option(c.writeCapacityUnits(): java.lang.Double).map(_.doubleValue()),
      localSecondaryIndexes =
        if (c.hasLocalSecondaryIndexes)
          c.localSecondaryIndexes.asScala.map { case (k, v) => k -> fromAwsCapacity(v) }.toMap
        else Map.empty,
      globalSecondaryIndexes =
        if (c.hasGlobalSecondaryIndexes)
          c.globalSecondaryIndexes.asScala.map { case (k, v) => k -> fromAwsCapacity(v) }.toMap
        else Map.empty
    )

  /** Converts an AWS SDK [[AwsItemCollectionMetrics]] to the library's [[ItemCollectionMetrics]]. */
  private[dynamodb] def fromAwsItemCollectionMetrics(
    m: AwsItemCollectionMetrics
  ): zio.dynamodb.ItemCollectionMetrics = {
    val sizes = if (m.hasSizeEstimateRangeGB) m.sizeEstimateRangeGB().asScala.toList else Nil
    zio.dynamodb.ItemCollectionMetrics(
      itemCollectionKey = if (m.hasItemCollectionKey) Some(fromAwsItem(m.itemCollectionKey())) else None,
      sizeEstimateRangeGB = (
        sizes.headOption.map(_.doubleValue()).getOrElse(0.0),
        sizes.drop(1).headOption.map(_.doubleValue()).getOrElse(0.0)
      )
    )
  }

  def toBatchWriteItemRequest(q: DynamoDBQuery.BatchWriteItem): BatchWriteItemRequest = {
    val requestItems = q.requestItems.iterator.map { case (tableName, writes) =>
      tableName -> writes.toList.map {
        case DynamoDBQuery.BatchWriteItem.Put(item)   =>
          WriteRequest
            .builder()
            .putRequest(PutRequest.builder().item(toAwsItem(item)).build())
            .build()
        case DynamoDBQuery.BatchWriteItem.Delete(key) =>
          WriteRequest
            .builder()
            .deleteRequest(DeleteRequest.builder().key(toAwsItem(key)).build())
            .build()
      }.asJava
    }.toMap.asJava
    BatchWriteItemRequest
      .builder()
      .requestItems(requestItems)
      .returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
      .returnItemCollectionMetrics(toAwsReturnItemCollectionMetrics(q.itemMetrics))
      .build()
  }

  def fromBatchWriteItemResponse(r: BatchWriteItemResponse): DynamoDBQuery.BatchWriteItem.Response = {
    val unprocessed =
      if (!r.hasUnprocessedItems || r.unprocessedItems.isEmpty)
        None
      else
        Some(
          r.unprocessedItems.asScala.foldLeft(MapOfSet.empty[String, DynamoDBQuery.BatchWriteItem.Write]) {
            case (acc, (tableName, writeRequests)) =>
              writeRequests.asScala.foldLeft(acc) { (acc2, wr) =>
                if (wr.putRequest != null)
                  acc2 + (tableName -> DynamoDBQuery.BatchWriteItem.Put(fromAwsItem(wr.putRequest.item)))
                else if (wr.deleteRequest != null)
                  acc2 + (tableName -> DynamoDBQuery.BatchWriteItem.Delete(fromAwsItem(wr.deleteRequest.key)))
                else acc2
              }
          }
        )
    DynamoDBQuery.BatchWriteItem.Response(unprocessed)
  }

  def toBatchGetItemRequest(q: DynamoDBQuery.BatchGetItem): BatchGetItemRequest = {
    val requestItems = q.requestItems.map { case (tableName, tableGet) =>
      val keys = tableGet.keysSet.toList.map(key => toAwsItem(key)).asJava
      val b    = KeysAndAttributes.builder().keys(keys)
      if (tableGet.projectionExpressionSet.nonEmpty) {
        val (aliasMap, projections) = AliasMapRender.forEach(tableGet.projectionExpressionSet.toList).execute
        val names                   = aliasMap.map.iterator.collect { case (AliasMap.PathSegment(_, segment), alias) =>
          alias -> segment
        }.toMap
        b.projectionExpression(projections.mkString(", "))
        if (names.nonEmpty) b.expressionAttributeNames(names.asJava)
      }
      tableName -> b.build()
    }.asJava
    BatchGetItemRequest
      .builder()
      .requestItems(requestItems)
      .returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
      .build()
  }

  def fromBatchGetItemResponse(r: BatchGetItemResponse): DynamoDBQuery.BatchGetItem.Response = {
    import scala.collection.immutable.{ Map => ScalaMap }
    val responses: MapOfSet[String, Item]                                      =
      if (!r.hasResponses || r.responses.isEmpty)
        MapOfSet.empty
      else
        r.responses.asScala.foldLeft(MapOfSet.empty[String, Item]) { case (acc, (tableName, items)) =>
          items.asScala.foldLeft(acc) { (acc2, item) =>
            acc2 + (tableName -> fromAwsItem(item))
          }
        }
    val unprocessedKeys: ScalaMap[String, DynamoDBQuery.BatchGetItem.TableGet] =
      if (!r.hasUnprocessedKeys || r.unprocessedKeys.isEmpty)
        ScalaMap.empty
      else
        r.unprocessedKeys.asScala.map { case (tableName, keysAndAttrs) =>
          val keys = keysAndAttrs.keys.asScala.map(fromAwsItem).toSet
          tableName -> DynamoDBQuery.BatchGetItem.TableGet(keys, Set.empty)
        }.toMap
    DynamoDBQuery.BatchGetItem.Response(responses, unprocessedKeys)
  }

  private def fromTableStatus(ts: AwsTableStatus): DynamoDBQuery.TableStatus =
    ts match {
      case AwsTableStatus.CREATING                            => DynamoDBQuery.TableStatus.Creating
      case AwsTableStatus.UPDATING                            => DynamoDBQuery.TableStatus.Updating
      case AwsTableStatus.DELETING                            => DynamoDBQuery.TableStatus.Deleting
      case AwsTableStatus.ACTIVE                              => DynamoDBQuery.TableStatus.Active
      case AwsTableStatus.INACCESSIBLE_ENCRYPTION_CREDENTIALS =>
        DynamoDBQuery.TableStatus.InaccessibleEncryptionCredentials
      case AwsTableStatus.ARCHIVING                           => DynamoDBQuery.TableStatus.Archiving
      case AwsTableStatus.ARCHIVED                            => DynamoDBQuery.TableStatus.Archived
      case _                                                  => DynamoDBQuery.TableStatus.unknownToSdkVersion
    }

  def fromDescribeTableResponse(r: AwsDescribeTableResponse): DynamoDBQuery.DescribeTableResponse = {
    val desc = r.table()
    DynamoDBQuery.DescribeTableResponse(
      tableArn = desc.tableArn,
      tableStatus = fromTableStatus(desc.tableStatus),
      tableSizeBytes = desc.tableSizeBytes,
      itemCount = desc.itemCount
    )
  }

  // TransactGetItems
  def toTransactGetItemsRequest(q: DynamoDBQuery.TransactGetItems): TransactGetItemsRequest = {
    val transactItems = q.getItems.map { gi =>
      val (aliasMap, projections) = AliasMapRender.forEach(gi.projections.toList).execute
      val names                   = aliasMap.map.iterator.collect { case (AliasMap.PathSegment(_, segment), alias) =>
        alias -> segment
      }.toMap
      val gb                      = AwsTransactGet
        .builder()
        .tableName(gi.tableName)
        .key(toAwsItem(gi.key))
      if (projections.nonEmpty) gb.projectionExpression(projections.mkString(", "))
      if (names.nonEmpty) gb.expressionAttributeNames(names.asJava)
      TransactGetItem.builder().get(gb.build()).build()
    }.toList.asJava
    TransactGetItemsRequest
      .builder()
      .transactItems(transactItems)
      .returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
      .build()
  }

  def fromTransactGetItemsResponse(r: TransactGetItemsResponse): Chunk[Option[Item]] =
    if (r.hasResponses)
      Chunk.fromIterable(r.responses.asScala.map { ir =>
        if (ir.hasItem && !ir.item.isEmpty) Some(fromAwsItem(ir.item)) else None
      })
    else Chunk.empty

  // TransactWriteItems
  private def toAwsReturnValuesOnConditionCheckFailure(
    rv: ReturnValuesOnConditionCheckFailure
  ): AwsReturnValuesOnConditionCheckFailure =
    rv match {
      case ReturnValuesOnConditionCheckFailure.None   => AwsReturnValuesOnConditionCheckFailure.NONE
      case ReturnValuesOnConditionCheckFailure.AllOld => AwsReturnValuesOnConditionCheckFailure.ALL_OLD
    }

  private def toAwsTransactWriteItem(wi: DynamoDBQuery[Any, Any]): AwsTransactWriteItem =
    wi match {
      case p: DynamoDBQuery.PutItem =>
        val pb = AwsTransactPut
          .builder()
          .tableName(p.tableName)
          .item(toAwsItem(p.item))
        p.conditionExpression.foreach { ce =>
          val (exprStr, names, values) = renderExpression(ce)
          pb.conditionExpression(exprStr)
          if (names.nonEmpty) pb.expressionAttributeNames(names.asJava)
          if (values.nonEmpty) pb.expressionAttributeValues(values.asJava)
        }
        p.returnValuesOnConditionCheckFailure.foreach(rv =>
          pb.returnValuesOnConditionCheckFailure(toAwsReturnValuesOnConditionCheckFailure(rv))
        )
        AwsTransactWriteItem.builder().put(pb.build()).build()

      case u: DynamoDBQuery.UpdateItem =>
        val composed                            = for {
          updateStr <- u.updateExpression.render
          condStr   <- AliasMapRender.collectAll(u.conditionExpression.map(_.render))
        } yield (updateStr, condStr)
        val (aliasMap, (updateStr, condStrOpt)) = composed.execute
        val names                               = aliasMap.map.iterator.collect { case (AliasMap.PathSegment(_, segment), alias) =>
          alias -> segment
        }.toMap
        val values                              = aliasMap.map.iterator.collect { case (AliasMap.AttributeValueKey(av), alias) =>
          toAwsAttrValue(av).map(alias -> _)
        }.flatten.toMap
        val ub                                  = AwsTransactUpdate
          .builder()
          .tableName(u.tableName)
          .key(toAwsItem(u.key))
          .updateExpression(updateStr)
        condStrOpt.foreach(ub.conditionExpression(_))
        if (names.nonEmpty) ub.expressionAttributeNames(names.asJava)
        if (values.nonEmpty) ub.expressionAttributeValues(values.asJava)
        u.returnValuesOnConditionCheckFailure.foreach(rv =>
          ub.returnValuesOnConditionCheckFailure(toAwsReturnValuesOnConditionCheckFailure(rv))
        )
        AwsTransactWriteItem.builder().update(ub.build()).build()

      case d: DynamoDBQuery.DeleteItem =>
        val db = AwsTransactDelete
          .builder()
          .tableName(d.tableName)
          .key(toAwsItem(d.key))
        d.conditionExpression.foreach { ce =>
          val (exprStr, names, values) = renderExpression(ce)
          db.conditionExpression(exprStr)
          if (names.nonEmpty) db.expressionAttributeNames(names.asJava)
          if (values.nonEmpty) db.expressionAttributeValues(values.asJava)
        }
        d.returnValuesOnConditionCheckFailure.foreach(rv =>
          db.returnValuesOnConditionCheckFailure(toAwsReturnValuesOnConditionCheckFailure(rv))
        )
        AwsTransactWriteItem.builder().delete(db.build()).build()

      case c: DynamoDBQuery.ConditionCheck =>
        val (exprStr, names, values) = renderExpression(c.conditionExpression)
        val cb                       = AwsConditionCheck
          .builder()
          .tableName(c.tableName)
          .key(toAwsItem(c.key))
          .conditionExpression(exprStr)
        if (names.nonEmpty) cb.expressionAttributeNames(names.asJava)
        if (values.nonEmpty) cb.expressionAttributeValues(values.asJava)
        c.returnValuesOnConditionCheckFailure.foreach(rv =>
          cb.returnValuesOnConditionCheckFailure(toAwsReturnValuesOnConditionCheckFailure(rv))
        )
        AwsTransactWriteItem.builder().conditionCheck(cb.build()).build()
    }

  def toTransactWriteItemsRequest(q: DynamoDBQuery.TransactWriteItems): TransactWriteItemsRequest = {
    val b = TransactWriteItemsRequest
      .builder()
      .transactItems(q.writeItems.map(toAwsTransactWriteItem).toList.asJava)
      .returnConsumedCapacity(toAwsReturnConsumedCapacity(q.capacity))
      .returnItemCollectionMetrics(toAwsReturnItemCollectionMetrics(q.itemMetrics))
    q.clientRequestToken.foreach(b.clientRequestToken(_))
    b.build()
  }

  def fromTransactionCanceledException(
    e: TransactionCanceledException
  ): DynamoDBError.TransactionError.TransactionCancelled = {
    val reasons = Chunk.fromIterable(e.cancellationReasons.asScala.map { r =>
      DynamoDBError.CancellationReason(
        code = r.code,
        message = Option(r.message),
        item = if (r.hasItem && !r.item.isEmpty) Some(fromAwsItem(r.item)) else None
      )
    })
    DynamoDBError.TransactionError.TransactionCancelled(reasons)
  }
}

// -- Shared AWS interpreter ----------------------------------------------

/**
 * Fills in `AwsInterpreter`'s per-operation `runXxx` methods using [[AwsCodecs]] to
 * translate the ADT to/from AWS SDK request/response types and `client` to make the actual
 * call. Concrete interpreters (`ZioInterpreter`, `CEInterpreter`, `FutureInterpreter`) extend
 * this and only need to supply an [[AwsDynamoDB]]`[F]` client plus their effect type's
 * `pure`/`flatMap`/`product`/... primitives — everything AWS-request-shaped is shared here.
 */
abstract class RealAwsInterpreter[F[_]](client: AwsDynamoDB[F]) extends AwsInterpreter[F] {

  protected def runGetItem(q: DynamoDBQuery.GetItem): F[Option[Item]] =
    map(client.getItem(AwsCodecs.toGetItemRequest(q)))(AwsCodecs.fromGetItemResponse)

  protected def runPutItem(q: DynamoDBQuery.PutItem): F[Option[Item]] =
    map(client.putItem(AwsCodecs.toPutItemRequest(q)))(AwsCodecs.fromPutItemResponse)

  protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): F[Option[Item]] =
    map(client.updateItem(AwsCodecs.toUpdateItemRequest(q)))(AwsCodecs.fromUpdateItemResponse)

  protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): F[Option[Item]] =
    map(client.deleteItem(AwsCodecs.toDeleteItemRequest(q)))(AwsCodecs.fromDeleteItemResponse)

  protected def runQuery(q: DynamoDBQuery.Query): F[Page[Item]] =
    map(client.query(AwsCodecs.toQueryRequest(q)))(AwsCodecs.fromQueryResponse)

  protected def runScan(q: DynamoDBQuery.Scan): F[Page[Item]] =
    map(client.scan(AwsCodecs.toScanRequest(q)))(AwsCodecs.fromScanResponse)

  protected def runCreateTable(q: DynamoDBQuery.CreateTable): F[Unit] =
    map(client.createTable(AwsCodecs.toCreateTableRequest(q)))(_ => ())

  protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): F[Unit] =
    map(client.deleteTable(AwsCodecs.toDeleteTableRequest(q)))(_ => ())

  protected def runDescribeTable(q: DynamoDBQuery.DescribeTable): F[DynamoDBQuery.DescribeTableResponse] =
    map(client.describeTable(AwsCodecs.toDescribeTableRequest(q)))(AwsCodecs.fromDescribeTableResponse)

  protected def runBatchGetItem(q: DynamoDBQuery.BatchGetItem): F[DynamoDBQuery.BatchGetItem.Response] =
    map(client.batchGetItem(AwsCodecs.toBatchGetItemRequest(q)))(AwsCodecs.fromBatchGetItemResponse)

  protected def runBatchWriteItem(q: DynamoDBQuery.BatchWriteItem): F[DynamoDBQuery.BatchWriteItem.Response] =
    map(client.batchWriteItem(AwsCodecs.toBatchWriteItemRequest(q)))(AwsCodecs.fromBatchWriteItemResponse)

  protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): F[Chunk[Option[Item]]] =
    map(client.transactGetItems(AwsCodecs.toTransactGetItemsRequest(q)))(AwsCodecs.fromTransactGetItemsResponse)

  protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): F[Unit] =
    flatMap(attempt(client.transactWriteItems(AwsCodecs.toTransactWriteItemsRequest(q)))) {
      case Right(_)                              => pure(())
      case Left(e: TransactionCanceledException) => fail(AwsCodecs.fromTransactionCanceledException(e))
      case Left(t)                               => raiseError(t)
    }
}

// -- ZIO interpreter (outline — commented out) ---------------------------
// Add to build.sbt: "dev.zio" %% "zio" % zioVersion
//
// import zio._
// import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
//
// class ZioInterpreter(sdkClient: DynamoDbAsyncClient)
//     extends RealAwsInterpreter[Task](new AwsDynamoDB[Task] {
//       def getItem(req: GetItemRequest): Task[GetItemResponse] =
//         ZIO.fromCompletableFuture(sdkClient.getItem(req))
//       def putItem(req: PutItemRequest): Task[PutItemResponse] =
//         ZIO.fromCompletableFuture(sdkClient.putItem(req))
//       def updateItem(req: UpdateItemRequest): Task[UpdateItemResponse] =
//         ZIO.fromCompletableFuture(sdkClient.updateItem(req))
//       def query(req: QueryRequest): Task[QueryResponse] =
//         ZIO.fromCompletableFuture(sdkClient.query(req))
//       def queryAll(req: QueryRequest): Task[QueryResponse] =
//         ZIO.fromCompletableFuture(sdkClient.query(req))
//     }) {
//
//   protected def pure[A](a: A): Task[A]                               = ZIO.succeed(a)
//   protected def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.map(f)
//   protected def product[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)] = fa.zip(fb)
//   protected def fail[A](e: DynamoDBError): Task[A]                   = ZIO.fail(new RuntimeException(e.toString))
//   protected def absolve[A](fa: Task[Either[ItemError, A]]): Task[A]   =
//     fa.flatMap(ZIO.fromEither(_).mapError(e => new RuntimeException(e.toString)))
// }

// -- Cats Effect interpreter (outline — commented out) -------------------
// Add to build.sbt: "org.typelevel" %% "cats-effect" % catsEffectVersion
//
// import cats.effect.IO
// import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
//
// class CatsInterpreter(sdkClient: DynamoDbAsyncClient)
//     extends RealAwsInterpreter[IO](new AwsDynamoDB[IO] {
//       def getItem(req: GetItemRequest): IO[GetItemResponse] =
//         IO.fromCompletableFuture(IO(sdkClient.getItem(req)))
//       def putItem(req: PutItemRequest): IO[PutItemResponse] =
//         IO.fromCompletableFuture(IO(sdkClient.putItem(req)))
//       def updateItem(req: UpdateItemRequest): IO[UpdateItemResponse] =
//         IO.fromCompletableFuture(IO(sdkClient.updateItem(req)))
//       def query(req: QueryRequest): IO[QueryResponse] =
//         IO.fromCompletableFuture(IO(sdkClient.query(req)))
//       def queryAll(req: QueryRequest): IO[QueryResponse] =
//         IO.fromCompletableFuture(IO(sdkClient.query(req)))
//     }) {
//
//   protected def pure[A](a: A): IO[A]                                 = IO.pure(a)
//   protected def map[A, B](fa: IO[A])(f: A => B): IO[B]               = fa.map(f)
//   protected def product[A, B](fa: IO[A], fb: IO[B]): IO[(A, B)]      = (fa, fb).tupled
//   protected def fail[A](e: DynamoDBError): IO[A]                     = IO.raiseError(new RuntimeException(e.toString))
//   protected def absolve[A](fa: IO[Either[ItemError, A]]): IO[A]       =
//     fa.flatMap(IO.fromEither).adaptError(e => new RuntimeException(e.toString))
// }
