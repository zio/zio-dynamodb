package zio.dynamodb

import zio.ZIO
import zio.stream.ZStream

sealed trait DynamoDBQuery[+A] { self =>
  final def <*[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = zipLeft(that)

  final def *>[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = zipRight(that)

  final def <*>[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = self zip that

  /*
  trait {
    def execute[A](q: DynamoDBQuery): ZIO[Any, Exception, A]
  }
  - interface/service to execute - we delegate to
    - live // require AWS config
    - testing // IN Mem store
   */
  def execute: ZIO[Any, Exception, A] = ???

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamoDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = DynamoDBQuery.Zip(self, that)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)

}

object DynamoDBQuery {
  import scala.collection.{ Map => ScalaMap }

  final case class Succeed[A](value: () => A) extends DynamoDBQuery[A]

  final case class GetItem(
    key: PrimaryKey,
    tableName: TableName,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    projections: List[ProjectionExpression] =
      List.empty, // If no attribute names are specified, then all attributes are returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends DynamoDBQuery[Option[Item]]

  // Interestingly scan can be run in parallel using segment number and total segments fields
  // If running in parallel segment number must be used consistently with the paging token
  // I have removed these fields on the assumption that the library will take care of these concerns
  final case class Scan[R, E](
    tableName: TableName,
    indexName: IndexName,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    limit: Option[Int] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                         // if ProjectExpression supplied then only valid value is SpecificAttributes
    // there are 2 modes of getting stuff back
    // 1) client does not control paging so we return a None for LastEvaluatedKey
    // 2) client controls paging via Limit so we return the LastEvaluatedKey
  ) extends DynamoDBQuery[(ZStream[R, E, Item], LastEvaluatedKey)]

  final case class Query[R, E](
    tableName: TableName,
    indexName: IndexName,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    keyConditionExpression: KeyConditionExpression,
    limit: Option[Int] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                         // if ProjectExpression supplied then only valid value is SpecificAttributes
    // there are 2 modes of getting stuff back
    // 1) client does not control paging so we return a None for LastEvaluatedKey
    // 2) client controls paging via Limit so we return the LastEvaluatedKey
  ) extends DynamoDBQuery[(ZStream[R, E, Item], LastEvaluatedKey)]

  final case class PutItem(
    tableName: TableName,
    item: Item,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None // PutItem does not recognize any values other than NONE or ALL_OLD.
  ) extends DynamoDBQuery[Unit] // TODO: model response

  final case class UpdateItem(
    tableName: TableName,
    primaryKey: PrimaryKey,
    conditionExpression: Option[ConditionExpression] = None,
    updateExpression: Set[UpdateExpression] = Set.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None
  ) extends DynamoDBQuery[Unit] // TODO: model response

  final case class DeleteItem(
    tableName: TableName,
    key: PrimaryKey,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues =
      ReturnValues.None // DeleteItem does not recognize any values other than NONE or ALL_OLD.
  ) extends DynamoDBQuery[Unit] // TODO: model response

  final case class CreateTable(
    tableName: TableName,
    keySchema: KeySchema,
    attributeDefinitions: NonEmptySet[AttributeDefinition],
    billingMode: BillingMode = BillingMode.Provisioned,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty,
    provisionedThroughput: Option[ProvisionedThroughput] = None,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty // you can have up to 50 tags
  ) extends DynamoDBQuery[Unit] // TODO: model response

  final case class Zip[A, B](left: DynamoDBQuery[A], right: DynamoDBQuery[B]) extends DynamoDBQuery[(A, B)]
  final case class Map[A, B](query: DynamoDBQuery[A], mapper: A => B)         extends DynamoDBQuery[B]

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)
}
