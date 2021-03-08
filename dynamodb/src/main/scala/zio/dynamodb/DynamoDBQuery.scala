package zio.dynamodb

import zio.ZIO
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableItem
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.WriteItemsMap
import zio.stream.ZStream

sealed trait DynamoDBQuery[+A] { self =>
  final def <*[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = zipLeft(that)

  final def *>[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = zipRight(that)

  final def <*>[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = self zip that

  def execute: ZIO[Any, Exception, A] = ???

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamoDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = DynamoDBQuery.Zip(self, that)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)

}

object DynamoDBQuery {
  import scala.collection.immutable.{ Map => ScalaMap }

  sealed trait Constructor[+A] extends DynamoDBQuery[A]

  final case class Succeed[A](value: () => A) extends Constructor[A]

  final case class GetItem(
    key: PrimaryKey,
    tableName: TableName,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    projections: List[ProjectionExpression] =
      List.empty, // If no attribute names are specified, then all attributes are returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends Constructor[Option[Item]]

  // TODO: move out from here - it should not be publicly visible
  final case class BatchGetItem(
    requestItems: ScalaMap[TableName, BatchGetItem.TableItem],
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends DynamoDBQuery[BatchGetItem.Response] { self =>

    def +(getItem: GetItem): BatchGetItem =
      BatchGetItem(
        self.requestItems + ((getItem.tableName, TableItem(getItem.key, getItem.projections))),
        self.capacity
      )

    def ++(that: BatchGetItem): BatchGetItem =
      BatchGetItem(self.requestItems ++ that.requestItems, self.capacity)
  }
  object BatchGetItem {
    final case class TableItem(
      key: PrimaryKey,
      projections: List[ProjectionExpression] =
        List.empty                                   // If no attribute names are specified, then all attributes are returned
    )
    final case class TableResponse(
      readConsistency: ConsistencyMode,
      expressionAttributeNames: Map[String, String], // for use with projections expression
      keys: PrimaryKey,
      projections: List[ProjectionExpression] =
        List.empty                                   // If no attribute names are specified, then all attributes are returned
    )
    final case class Response(
      // TODO: return metadata
      responses: ScalaMap[TableName, Item],
      unprocessedKeys: ScalaMap[TableName, TableResponse]
    )
  }

  // TODO: move out from here - it should not be publicly visible
  final case class BatchWriteItem(
    requestItems: WriteItemsMap,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None
  ) extends DynamoDBQuery[BatchWriteItem.Response] { self =>

    def ++(that: BatchWriteItem): BatchWriteItem =
      BatchWriteItem(self.requestItems ++ that.requestItems, self.capacity, self.itemMetrics)
  }
  object BatchWriteItem {
    sealed trait Write
    final case class Delete(key: PrimaryKey) extends Write
    final case class Put(item: Item)         extends Write

    final case class Response(
      // TODO: return metadata
      responses: ScalaMap[TableName, Item],
      unprocessedKeys: Map[TableName, BatchWriteItem.Write]
    )

    final case class WriteItemsMap(map: ScalaMap[TableName, Set[BatchWriteItem.Write]] = ScalaMap.empty) { self =>
      def +(entry: (TableName, BatchWriteItem.Write)): WriteItemsMap = {
        val newEntry =
          map.get(entry._1).fold((entry._1, Set(entry._2)))(set => (entry._1, set + entry._2))
        WriteItemsMap(map + newEntry)
      }
      def ++(that: WriteItemsMap): WriteItemsMap = {
        val xs: Seq[(TableName, Set[BatchWriteItem.Write])]   = that.map.toList
        val m: ScalaMap[TableName, Set[BatchWriteItem.Write]] = xs.foldRight(map) {
          case ((tableName, set), map) =>
            val newEntry: (TableName, Set[BatchWriteItem.Write]) =
              map.get(tableName).fold((tableName, set))(s => (tableName, s ++ set))
            map + newEntry
        }
        WriteItemsMap(m)
      }
    }
    object WriteItemsMap                                                                                 {
      def empty: WriteItemsMap = WriteItemsMap(ScalaMap.empty)
    }
  }

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
  ) extends Constructor[(ZStream[R, E, Item], LastEvaluatedKey)]

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
  ) extends Constructor[(ZStream[R, E, Item], LastEvaluatedKey)]

  final case class PutItem(
    tableName: TableName,
    item: Item,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None // PutItem does not recognize any values other than NONE or ALL_OLD.
  ) extends Constructor[Unit] // TODO: model response

  final case class UpdateItem(
    tableName: TableName,
    primaryKey: PrimaryKey,
    conditionExpression: Option[ConditionExpression] = None,
    updateExpression: Set[UpdateExpression] = Set.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None
  ) extends Constructor[Unit] // TODO: model response

  final case class DeleteItem(
    tableName: TableName,
    key: PrimaryKey,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues =
      ReturnValues.None // DeleteItem does not recognize any values other than NONE or ALL_OLD.
  ) extends Constructor[Unit] // TODO: model response

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
  ) extends Constructor[Unit] // TODO: model response

  final case class Zip[A, B](left: DynamoDBQuery[A], right: DynamoDBQuery[B]) extends DynamoDBQuery[(A, B)]
  final case class Map[A, B](query: DynamoDBQuery[A], mapper: A => B)         extends DynamoDBQuery[B]

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)
}
