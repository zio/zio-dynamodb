package zio.dynamodb

import zio.Chunk
import zio.stream.ZStream

sealed trait DynamoDBQuery[+A] { self =>
  final def <*[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = zipLeft(that)

  final def *>[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = zipRight(that)

  final def <*>[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = self zip that

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamoDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = DynamoDBQuery.Zip(self, that)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)
}

object DynamoDBQuery {
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
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    filterExpression: Option[FilterExpression] = None,    // TODO: should we push NONE into FilterExpression?
    indexName: IndexName,
    limit: Option[Int] = None,                            // One based
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    segment: Option[ScanSegments] = None,
    select: Option[Select],                               // ProjectExpression supplied then only valid value is SpecificAttributes
    tableName: TableName
  ) extends DynamoDBQuery[ZStream[R, E, Item]]
  final case class PutItem(
    conditionExpression: ConditionExpression,
    item: Item,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
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
