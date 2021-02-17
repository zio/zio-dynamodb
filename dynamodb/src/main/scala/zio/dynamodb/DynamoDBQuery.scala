package zio.dynamodb

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
  // Filter expression is the same as a ConditionExpression but when used with Query but does not allow key attributes
  type FilterExpression = ConditionExpression

  final case class Succeed[A](value: () => A) extends DynamoDBQuery[A]
  final case class GetItem(
    key: PrimaryKey,
    tableName: TableName,
    readConsistency: ConsistencyMode =
      ConsistencyMode.Weak, // use Option type or default like here? - not sure what is best - defaulting lets interpreter be more dumb
    projections: List[ProjectionExpression] =
      List.empty,           // If no attribute names are specified, then all attributes are returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends DynamoDBQuery[Item]
  // Interestingly scan can be run in parallel using segment number and total segments fields
  // If running in parallel segment number must be used consistently with the paging token
  // I have removed these fields on the assumption that the library will take care of these concerns
  final case class Scan[R, E](
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    filterExpression: Option[FilterExpression] = None,    // TODO: should we push NONE into FilterExpression?
    indexName: IndexName,
    limit: Option[Int] = None,                            // One based
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select],                               // if ProjectExpression supplied then only valid value is SpecificAttributes
    tableName: TableName
  ) extends DynamoDBQuery[ZStream[R, E, Item]]
  // KeyCondition expression is aq restricted version of ConditionExpression where by
  // - partition exprn is required
  // - optionaly AND can be used sort key expression
  // eg partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
  // comparisons are the same as for Condition
  final case class Query[R, E](
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    filterExpression: Option[FilterExpression] = None,
    indexName: IndexName,
    keyConditionExpression: KeyConditionExpression,
    limit: Option[Int] = None,                            // One based
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select],                               // if ProjectExpression supplied then only valid value is SpecificAttributes
    tableName: TableName
  ) extends DynamoDBQuery[ZStream[R, E, Item]]
  final case class PutItem(
    conditionExpression: Option[ConditionExpression] = None, // TODO: we could use a True constant ConditionExpression
    item: Item,
    tableName: TableName,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None           // PutItem does not recognize any values other than NONE or ALL_OLD.
  ) extends DynamoDBQuery[Unit] // TODO: how do we model responses to DB mutations? AWS has a rich response model
  final case class UpdateItem(
    conditionExpression: Option[ConditionExpression] = None,
    primaryKey: PrimaryKey,
    tableName: TableName,
    updateExpression: Set[UpdateExpression] = Set.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None
  ) extends DynamoDBQuery[Unit] // TODO: how do we model responses to DB mutations? AWS has a rich response model
  final case class DeleteItem(
    conditionExpression: Option[ConditionExpression] = None,
    primaryKey: PrimaryKey,
    tableName: TableName,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues =
      ReturnValues.None // DeleteItem does not recognize any values other than NONE or ALL_OLD.
  ) extends DynamoDBQuery[Unit] // TODO: how do we model responses to DB mutations? AWS has a rich response model

  final case class Zip[A, B](left: DynamoDBQuery[A], right: DynamoDBQuery[B]) extends DynamoDBQuery[(A, B)]
  final case class Map[A, B](query: DynamoDBQuery[A], mapper: A => B)         extends DynamoDBQuery[B]

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)
}
