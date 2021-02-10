package zio.dynamodb

import zio.Chunk

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
    readConsistency: ConsistencyMode,
    projections: List[ProjectionExpression],
    capacity: ReturnConsumedCapacity
  )                                           extends DynamoDBQuery[Chunk[Byte]]
  final case class Scan(
    readConsistency: ConsistencyMode,
    exclusiveStartKey: ExclusiveStartKey,
    filterExpression: FilterExpression,
    indexName: IndexName,
    limit: Int,        // One based
    projections: List[ProjectionExpression],
    capacity: ReturnConsumedCapacity,
    segment: Int,      // zero based
    select: Select,
    tableName: TableName,
    totalSegments: Int // optional
  ) extends DynamoDBQuery[Chunk[Byte]]
  final case class PutItem(
    conditionExpression: ConditionExpression,
    item: Item,
    capacity: ReturnConsumedCapacity,
    itemMetrics: ReturnItemCollectionMetrics,
    returnValues: ReturnValues,
    tableName: TableName
  ) extends DynamoDBQuery[Chunk[Byte]]
  final case class UpdateItem(
    conditionExpression: ConditionExpression,
    primaryKey: PrimaryKey,
    capacity: ReturnConsumedCapacity,
    itemMetrics: ReturnItemCollectionMetrics,
    returnValues: ReturnValues,
    tableName: TableName,
    updateExpression: String // TODO
  ) extends DynamoDBQuery[Chunk[Byte]]

  final case class Zip[A, B](left: DynamoDBQuery[A], right: DynamoDBQuery[B]) extends DynamoDBQuery[(A, B)]
  final case class Map[A, B](query: DynamoDBQuery[A], mapper: A => B)         extends DynamoDBQuery[B]

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)
}
