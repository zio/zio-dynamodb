package zio.dynamodb

import zio.blocks.chunk.Chunk

/**
 * Capacity consumed by a single component (base table, LSI, or GSI).
 *  `capacityUnits` is the total; `readCapacityUnits` and `writeCapacityUnits`
 *  are only non-empty when the operation is a read or write respectively.
 */
final case class Capacity(
  readCapacityUnits: Option[Double],
  writeCapacityUnits: Option[Double],
  capacityUnits: Option[Double]
)

/**
 * Consumed capacity reported by AWS for a single DynamoDB operation. All fields
 *  are optional; they are only populated when [[ReturnConsumedCapacity]] is set
 *  on the request, which [[InterceptingAwsDynamoDB]] does automatically.
 *  `localSecondaryIndexes` and `globalSecondaryIndexes` are keyed by index name
 *  and are empty maps when no index capacity was consumed or reported.
 */
final case class ConsumedCapacity(
  tableName: Option[String],
  readCapacityUnits: Option[Double],
  writeCapacityUnits: Option[Double],
  localSecondaryIndexes: Map[String, Capacity],
  globalSecondaryIndexes: Map[String, Capacity]
)

/**
 * Item collection size estimate returned by AWS for write operations on a table
 *  with a local secondary index.
 */
final case class ItemCollectionMetrics(
  itemCollectionKey: Option[Item],
  sizeEstimateRangeGB: (Double, Double)
)

/**
 * Primary key extracted from the originating request. Populated for GetItem,
 *  UpdateItem, and DeleteItem (where the key is an explicit request field).
 *  `primaryKey` is [[None]] for PutItem because the key fields are embedded in
 *  the full item map and cannot be separated without table schema knowledge.
 */
final case class CorrelationContext(primaryKey: Option[PrimaryKey])

/**
 * AWS response metadata produced after each DynamoDB operation when an
 *  interceptor is attached. The sealed subtype encodes the operation type.
 *  DDL operations (CreateTable, DeleteTable, DescribeTable) produce no metadata
 *  and do not invoke the interceptor.
 */
sealed trait DynamoDBResponseMetadata

object DynamoDBResponseMetadata {

  /** Metadata for a GetItem operation. */
  final case class GetItem(
    tableName: String,
    consumed: Option[ConsumedCapacity],
    correlation: CorrelationContext
  ) extends DynamoDBResponseMetadata

  /** Metadata for a PutItem operation. */
  final case class PutItem(
    tableName: String,
    consumed: Option[ConsumedCapacity],
    collectionMetrics: Option[ItemCollectionMetrics],
    correlation: CorrelationContext
  ) extends DynamoDBResponseMetadata

  /** Metadata for an UpdateItem operation. */
  final case class UpdateItem(
    tableName: String,
    consumed: Option[ConsumedCapacity],
    collectionMetrics: Option[ItemCollectionMetrics],
    correlation: CorrelationContext
  ) extends DynamoDBResponseMetadata

  /** Metadata for a DeleteItem operation. */
  final case class DeleteItem(
    tableName: String,
    consumed: Option[ConsumedCapacity],
    collectionMetrics: Option[ItemCollectionMetrics],
    correlation: CorrelationContext
  ) extends DynamoDBResponseMetadata

  /** Metadata for a Query (querySome) operation. */
  final case class Query(
    tableName: String,
    consumed: Option[ConsumedCapacity]
  ) extends DynamoDBResponseMetadata

  /** Metadata for a Scan (scanSome) operation. */
  final case class Scan(
    tableName: String,
    consumed: Option[ConsumedCapacity]
  ) extends DynamoDBResponseMetadata

  /** Consumed capacity per table involved in the batch. */
  final case class BatchGetItem(consumed: Chunk[ConsumedCapacity]) extends DynamoDBResponseMetadata

  /** Consumed capacity per table involved in the batch. */
  final case class BatchWriteItem(consumed: Chunk[ConsumedCapacity]) extends DynamoDBResponseMetadata

  /** Consumed capacity per table involved in a transactional read. */
  final case class TransactGetItems(consumed: Chunk[ConsumedCapacity]) extends DynamoDBResponseMetadata

  /** Consumed capacity per table involved in a transactional write. */
  final case class TransactWriteItems(consumed: Chunk[ConsumedCapacity]) extends DynamoDBResponseMetadata
}
