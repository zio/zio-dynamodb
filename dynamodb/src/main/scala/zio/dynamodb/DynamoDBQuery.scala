package zio.dynamodb

import zio.Chunk
import zio.stream.ZStream

sealed trait DynamoDBQuery[+A] { self =>
  final def <*[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = zipLeft(that)

  final def *>[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = zipRight(that)

  final def <*>[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = self zip that

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamicDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = DynamicDBQuery.Zip(self, that)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)
}

object DynamicDBQuery {
  type FilterExpression = ConditionExpression

  final case class Succeed[A](value: () => A) extends DynamoDBQuery[A]
  final case class GetItem(
    key: PrimaryKey,
    tableName: TableName,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    projections: List[ProjectionExpression] =
      List.empty, // If no attribute names are specified, then all attributes are returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends DynamoDBQuery[Item]
  final case class Scan[R, E](
    readConsistency: ConsistencyMode,
    exclusiveStartKey: ExclusiveStartKey, // should the library take care of token based pagination? if so then pagination details would not be exposed
    filterExpression: FilterExpression,
    indexName: IndexName,
    limit: Int,                           // One based
    projections: List[ProjectionExpression],
    capacity: ReturnConsumedCapacity,
    segment: Int,                         // zero based. For a parallel Scan request, Segment identifies an individual segment to be scanned by an application worker.
    select: Select,
    tableName: TableName,
    totalSegments: Int                    // For a parallel Scan request, TotalSegments represents the total number of segments into which the Scan operation will be divided.
  ) extends DynamoDBQuery[ZStream[R, E, Item]]
  final case class PutItem(
    conditionExpression: ConditionExpression,
    item: Item,
    capacity: ReturnConsumedCapacity,
    itemMetrics: ReturnItemCollectionMetrics,
    returnValues: ReturnValues,
    tableName: TableName
  ) extends DynamoDBQuery[Chunk[Byte]] // TODO: how do we model responses to DB mutations? AWS has a rich response model
  final case class UpdateItem(
    conditionExpression: ConditionExpression,
    primaryKey: PrimaryKey,
    capacity: ReturnConsumedCapacity,
    itemMetrics: ReturnItemCollectionMetrics,
    returnValues: ReturnValues,
    tableName: TableName,
    updateExpression: UpdateExpression
  ) extends DynamoDBQuery[Chunk[Byte]] // TODO: how do we model responses to DB mutations? AWS has a rich response model

  final case class Zip[A, B](left: DynamoDBQuery[A], right: DynamoDBQuery[B]) extends DynamoDBQuery[(A, B)]
  final case class Map[A, B](query: DynamoDBQuery[A], mapper: A => B)         extends DynamoDBQuery[B]

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)
}
