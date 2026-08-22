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

import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemRequest,
  BatchGetItemResponse,
  BatchWriteItemRequest,
  BatchWriteItemResponse,
  CreateTableRequest,
  CreateTableResponse,
  DeleteItemRequest,
  DeleteItemResponse,
  DeleteTableRequest,
  DeleteTableResponse,
  DescribeTableRequest,
  DescribeTableResponse => AwsDescribeTableResponse,
  GetItemRequest,
  GetItemResponse,
  PutItemRequest,
  PutItemResponse,
  QueryRequest,
  QueryResponse,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  ScanRequest,
  ScanResponse,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  UpdateItemRequest,
  UpdateItemResponse
}
import zio.blocks.chunk.Chunk

import scala.collection.JavaConverters._

/**
 * Wraps an [[AwsDynamoDB]] to enrich every data-operation request with
 *  `ReturnConsumedCapacity.TOTAL` (and `ReturnItemCollectionMetrics.SIZE` for
 *  writes), then fires `interceptor` after each response.
 *
 *  DDL operations (createTable, deleteTable, describeTable) are delegated
 *  directly to `inner` without touching the interceptor.
 */
private[dynamodb] final class InterceptingAwsDynamoDB[F[_]](
  inner: AwsDynamoDB[F],
  interceptor: ResponseInterceptor[F],
  ops: EffectOps[F]
) extends AwsDynamoDB[F] {

  def getItem(req: GetItemRequest): F[GetItemResponse] = {
    val enriched = req.toBuilder.returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build()
    ops.flatMap(inner.getItem(enriched)) { resp =>
      val correlation = CorrelationContext(primaryKey = Some(AwsCodecs.fromAwsItem(req.key)))
      val meta        = DynamoDBResponseMetadata.GetItem(
        tableName = req.tableName,
        consumed = Option(resp.consumedCapacity).map(AwsCodecs.fromAwsConsumedCapacity),
        correlation = correlation
      )
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def putItem(req: PutItemRequest): F[PutItemResponse] = {
    val enriched = req.toBuilder
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
      .build()
    ops.flatMap(inner.putItem(enriched)) { resp =>
      // PK fields are embedded in req.item() alongside all other attributes;
      // isolating them requires table schema knowledge, so primaryKey = None.
      val correlation = CorrelationContext(primaryKey = None)
      val meta        = DynamoDBResponseMetadata.PutItem(
        tableName = req.tableName,
        consumed = Option(resp.consumedCapacity).map(AwsCodecs.fromAwsConsumedCapacity),
        collectionMetrics = Option(resp.itemCollectionMetrics).map(AwsCodecs.fromAwsItemCollectionMetrics),
        correlation = correlation
      )
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def updateItem(req: UpdateItemRequest): F[UpdateItemResponse] = {
    val enriched = req.toBuilder
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
      .build()
    ops.flatMap(inner.updateItem(enriched)) { resp =>
      val correlation = CorrelationContext(primaryKey = Some(AwsCodecs.fromAwsItem(req.key)))
      val meta        = DynamoDBResponseMetadata.UpdateItem(
        tableName = req.tableName,
        consumed = Option(resp.consumedCapacity).map(AwsCodecs.fromAwsConsumedCapacity),
        collectionMetrics = Option(resp.itemCollectionMetrics).map(AwsCodecs.fromAwsItemCollectionMetrics),
        correlation = correlation
      )
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def deleteItem(req: DeleteItemRequest): F[DeleteItemResponse] = {
    val enriched = req.toBuilder
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
      .build()
    ops.flatMap(inner.deleteItem(enriched)) { resp =>
      val correlation = CorrelationContext(primaryKey = Some(AwsCodecs.fromAwsItem(req.key)))
      val meta        = DynamoDBResponseMetadata.DeleteItem(
        tableName = req.tableName,
        consumed = Option(resp.consumedCapacity).map(AwsCodecs.fromAwsConsumedCapacity),
        collectionMetrics = Option(resp.itemCollectionMetrics).map(AwsCodecs.fromAwsItemCollectionMetrics),
        correlation = correlation
      )
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def query(req: QueryRequest): F[QueryResponse] = {
    val enriched = req.toBuilder.returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build()
    ops.flatMap(inner.query(enriched)) { resp =>
      val meta = DynamoDBResponseMetadata.Query(
        tableName = req.tableName,
        consumed = Option(resp.consumedCapacity).map(AwsCodecs.fromAwsConsumedCapacity)
      )
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def scan(req: ScanRequest): F[ScanResponse] = {
    val enriched = req.toBuilder.returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build()
    ops.flatMap(inner.scan(enriched)) { resp =>
      val meta = DynamoDBResponseMetadata.Scan(
        tableName = req.tableName,
        consumed = Option(resp.consumedCapacity).map(AwsCodecs.fromAwsConsumedCapacity)
      )
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def batchGetItem(req: BatchGetItemRequest): F[BatchGetItemResponse] = {
    val enriched = req.toBuilder.returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build()
    ops.flatMap(inner.batchGetItem(enriched)) { resp =>
      val consumed =
        if (resp.hasConsumedCapacity)
          Chunk.fromIterable(resp.consumedCapacity.asScala.map(AwsCodecs.fromAwsConsumedCapacity))
        else Chunk.empty[ConsumedCapacity]
      val meta     = DynamoDBResponseMetadata.BatchGetItem(consumed)
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def batchWriteItem(req: BatchWriteItemRequest): F[BatchWriteItemResponse] = {
    val enriched = req.toBuilder
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
      .build()
    ops.flatMap(inner.batchWriteItem(enriched)) { resp =>
      val consumed =
        if (resp.hasConsumedCapacity)
          Chunk.fromIterable(resp.consumedCapacity.asScala.map(AwsCodecs.fromAwsConsumedCapacity))
        else Chunk.empty[ConsumedCapacity]
      val meta     = DynamoDBResponseMetadata.BatchWriteItem(consumed)
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def transactGetItems(req: TransactGetItemsRequest): F[TransactGetItemsResponse] = {
    val enriched = req.toBuilder.returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build()
    ops.flatMap(inner.transactGetItems(enriched)) { resp =>
      val consumed =
        if (resp.hasConsumedCapacity)
          Chunk.fromIterable(resp.consumedCapacity.asScala.map(AwsCodecs.fromAwsConsumedCapacity))
        else Chunk.empty[ConsumedCapacity]
      val meta     = DynamoDBResponseMetadata.TransactGetItems(consumed)
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def transactWriteItems(req: TransactWriteItemsRequest): F[TransactWriteItemsResponse] = {
    val enriched = req.toBuilder
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
      .build()
    ops.flatMap(inner.transactWriteItems(enriched)) { resp =>
      val consumed =
        if (resp.hasConsumedCapacity)
          Chunk.fromIterable(resp.consumedCapacity.asScala.map(AwsCodecs.fromAwsConsumedCapacity))
        else Chunk.empty[ConsumedCapacity]
      val meta     = DynamoDBResponseMetadata.TransactWriteItems(consumed)
      ops.map(interceptor.onResponse(meta))(_ => resp)
    }
  }

  def createTable(req: CreateTableRequest): F[CreateTableResponse]          = inner.createTable(req)
  def deleteTable(req: DeleteTableRequest): F[DeleteTableResponse]          = inner.deleteTable(req)
  def describeTable(req: DescribeTableRequest): F[AwsDescribeTableResponse] = inner.describeTable(req)
}
