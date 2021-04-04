package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.{ Delete, Put }
import zio.dynamodb.DynamoDBQuery.{
  batched,
  parallelize,
  BatchGetItem,
  BatchWriteItem,
  DeleteItem,
  GetItem,
  PutItem,
  QueryAll,
  QueryPage,
  ScanAll,
  ScanPage,
  UpdateItem
}
import zio.stream.Stream
import zio.{ Chunk, ZIO }

sealed trait DynamoDBQuery[+A] { self =>

  // if you subtype you avoid the cast
  final def capacity(capacity: ReturnConsumedCapacity): DynamoDBQuery[A] =
    self match {
      case g: GetItem        =>
        g.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case b: BatchGetItem   =>
        b.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case b: BatchWriteItem =>
        b.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case q: ScanAll        =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case q: ScanPage       =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case q: QueryAll       =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case q: QueryPage      =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case m: PutItem        =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case m: UpdateItem     =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case m: DeleteItem     =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case _                 => self
    }

  final def consistency(consistency: ConsistencyMode): DynamoDBQuery[A] =
    self match {
      case g: GetItem   =>
        g.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: ScanAll   =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: ScanPage  =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: QueryAll  =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: QueryPage =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case _            => self
    }

  final def <*[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = zipLeft(that)

  final def *>[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = zipRight(that)

  final def <*>[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = self zip that

  def execute: ZIO[DynamoDBExecutor, Exception, A] = {
    val (constructors, assembler)                                                                   = parallelize(self)
    val (indexedConstructors, (batchGetItem, batchGetIndexes), (batchWriteItem, batchWriteIndexes)) =
      batched(constructors)

    val indexedNonBatchedResults =
      ZIO.foreachPar(indexedConstructors) {
        case (constructor, index) =>
          ddbExecute(constructor).map(result => (result, index))
      }

    val indexedGetResults =
      ddbExecute(batchGetItem).map(resp => batchGetItem.toGetItemResponses(resp) zip batchGetIndexes)

    val indexedWriteResults =
      // TODO: think about mapping return values from writes
      ddbExecute(batchWriteItem).map(_ => batchWriteItem.addList.map(_ => ()) zip batchWriteIndexes)

    (indexedNonBatchedResults zipPar indexedGetResults zipPar indexedWriteResults).map {
      case ((nonBatched, batchedGets), batchedWrites) =>
        val combined = (nonBatched ++ batchedGets ++ batchedWrites).sortBy {
          case (_, index) => index
        }.map(_._1)
        assembler(combined)
    }

  }

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamoDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = DynamoDBQuery.Zip(self, that)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)

  final def zipWith[B, C](that: DynamoDBQuery[B])(f: (A, B) => C): DynamoDBQuery[C] =
    self.zip(that).map(f.tupled)

}

object DynamoDBQuery {
  import scala.collection.immutable.{ Map => ScalaMap }

  sealed trait Constructor[+A] extends DynamoDBQuery[A]
  sealed trait Write[+A]       extends Constructor[A]

  final case class Succeed[A](value: A) extends Constructor[A]

  def succeed[A](a: A): DynamoDBQuery[A] = Succeed(a)

  /*
   var args made possible by factoring out parameters
   */
  def getItem(
    tableName: TableName,
    key: PrimaryKey,
    projections: ProjectionExpression*
  ): DynamoDBQuery[Option[Item]] = {
    println(projections)
    GetItem(tableName, key)
  }

  def forEach[A, B](values: Iterable[A])(body: A => DynamoDBQuery[B]): DynamoDBQuery[List[B]] =
    values.foldRight[DynamoDBQuery[List[B]]](succeed(Nil)) {
      // from each element
      // start with accumulator succeed(Nil)
      // 1. produce a query by passing the element to the function
      // 2. produce a single query by cons list inside succeed with query
      // 3. as this is a fold we end up with a single Query of list
      case (a, query) => body(a).zipWith(query)(_ :: _)
    }

  /*
   for returnValues create def returns(returnValues: ReturnValues):
   for conditionExpression create def where(conditionExpression: ConditionExpression):
   for metrics create def where(conditionExpression: ConditionExpression):
   */

  def putItem(tableName: TableName, item: Item): DynamoDBQuery[Unit] = PutItem(tableName, item)

  final case class GetItem(
    tableName: TableName,
    key: PrimaryKey,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    projections: List[ProjectionExpression] =
      List.empty, // If no attribute names are specified, then all attributes are returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends Constructor[Option[Item]]

  // TODO: should this be publicly visible?
  final case class BatchGetItem(
    requestItems: MapOfSet[TableName, BatchGetItem.TableGet] = MapOfSet.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    addList: Chunk[GetItem] = Chunk.empty // track order of added GetItems for later unpacking
  ) extends Constructor[BatchGetItem.Response] { self =>

    def +(getItem: GetItem): BatchGetItem =
      BatchGetItem(
        self.requestItems + (getItem.tableName -> TableGet(getItem.key, getItem.projections)),
        self.capacity,
        self.addList :+ getItem
      )

    def addAll(entries: GetItem*): BatchGetItem =
      entries.foldLeft(self) {
        case (batch, getItem) => batch + getItem
      }

    /*
     for each added GetItem, check it's key exists in the response and create a corresponding Optional Item value
     */
    def toGetItemResponses(response: BatchGetItem.Response): Chunk[Option[Item]] = {
      val chunk: Chunk[Option[Item]] = addList.foldLeft[Chunk[Option[Item]]](Chunk.empty) {
        case (chunk, getItem) =>
          val responsesForTable: Set[Item] = response.responses.map.getOrElse(getItem.tableName, Set.empty[Item])
          val found: Option[Item]          = responsesForTable.find { item =>
            getItem.key.value.toSet.subsetOf(item.value.toSet)
          }

          found.fold(chunk :+ None)(item => chunk :+ Some(item))
      }

      chunk
    }

  }
  object BatchGetItem {
    final case class TableGet(
      key: PrimaryKey,
      projections: List[ProjectionExpression] =
        List.empty                                   // If no attribute names are specified, then all attributes are returned
    )
    final case class TableResponse(
      readConsistency: ConsistencyMode,
      expressionAttributeNames: Map[String, String], // for use with projections expression
      keys: Set[PrimaryKey] = Set.empty,
      projections: List[ProjectionExpression] =
        List.empty                                   // If no attribute names are specified, then all attributes are returned
    )
    final case class Response(
      // TODO: return metadata

      // Note - if a requested item does not exist, it is not returned in the result
      responses: MapOfSet[TableName, Item] = MapOfSet.empty,
      unprocessedKeys: ScalaMap[TableName, TableResponse] = ScalaMap.empty
    )
  }

  // TODO: should this be publicly visible?
  final case class BatchWriteItem(
    requestItems: MapOfSet[TableName, BatchWriteItem.Write] = MapOfSet.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    addList: Chunk[BatchWriteItem.Write] = Chunk.empty
  ) extends Constructor[BatchWriteItem.Response] { self =>
    def +[A](writeItem: Write[A]): BatchWriteItem =
      writeItem match {
        case putItem @ PutItem(_, _, _, _, _, _)       =>
          BatchWriteItem(
            self.requestItems + ((putItem.tableName, Put(putItem.item))),
            self.capacity,
            self.itemMetrics,
            self.addList :+ Put(putItem.item)
          )
        case deleteItem @ DeleteItem(_, _, _, _, _, _) =>
          BatchWriteItem(
            self.requestItems + ((deleteItem.tableName, Delete(deleteItem.key))),
            self.capacity,
            self.itemMetrics,
            self.addList :+ Delete(deleteItem.key)
          )
      }

    def addAll[A](entries: Write[A]*): BatchWriteItem =
      entries.foldLeft(self) {
        case (batch, write) => batch + write
      }

  }
  object BatchWriteItem {
    sealed trait Write
    final case class Delete(key: PrimaryKey) extends Write
    final case class Put(item: Item)         extends Write

    final case class Response(
      // TODO: return metadata
      unprocessedKeys: MapOfSet[TableName, BatchWriteItem.Write] = MapOfSet.empty
    )

  }

  // Interestingly scan can be run in parallel using segment number and total segments fields
  // If running in parallel segment number must be used consistently with the paging token
  // I have removed these fields on the assumption that the library will take care of these concerns
  final case class ScanPage(
    tableName: TableName,
    indexName: IndexName,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    limit: Option[Int] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                         // if ProjectExpression supplied then only valid value is SpecificAttributes
  ) extends Constructor[(Chunk[Item], LastEvaluatedKey)]

  final case class QueryPage(
    tableName: TableName,
    indexName: IndexName,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    keyConditionExpression: Option[KeyConditionExpression] = None,
    limit: Option[Int] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                         // if ProjectExpression supplied then only valid value is SpecificAttributes
  ) extends Constructor[(Chunk[Item], LastEvaluatedKey)]

  final case class ScanAll(
    tableName: TableName,
    indexName: IndexName,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                         // if ProjectExpression supplied then only valid value is SpecificAttributes
  ) extends Constructor[Stream[Exception, Item]]

  final case class QueryAll(
    tableName: TableName,
    indexName: IndexName,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    keyConditionExpression: Option[KeyConditionExpression] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                         // if ProjectExpression supplied then only valid value is SpecificAttributes
  ) extends Constructor[Stream[Exception, Item]]

  final case class PutItem(
    tableName: TableName,
    item: Item,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None // PutItem does not recognize any values other than NONE or ALL_OLD.
  ) extends Write[Unit] // TODO: model response

  final case class UpdateItem(
    tableName: TableName,
    key: PrimaryKey,
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
  ) extends Write[Unit] // TODO: model response

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

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(a)

  private[dynamodb] def batched(
    constructors: Chunk[Constructor[Any]]
  ): (Chunk[(Constructor[Any], Int)], (BatchGetItem, Chunk[Int]), (BatchWriteItem, Chunk[Int])) = {
    type IndexedConstructor = (Constructor[Any], Int)
    type IndexedGetItem     = (GetItem, Int)
    type IndexedWriteItem   = (Write[Unit], Int)

    val (nonBatched, gets, writes) =
      constructors.zipWithIndex.foldLeft[(Chunk[IndexedConstructor], Chunk[IndexedGetItem], Chunk[IndexedWriteItem])](
        (Chunk.empty, Chunk.empty, Chunk.empty)
      ) {
        case ((nonBatched, gets, writes), (get @ GetItem(_, _, _, _, _), index))          =>
          (nonBatched, gets :+ (get -> index), writes)
        case ((nonBatched, gets, writes), (put @ PutItem(_, _, _, _, _, _), index))       =>
          (nonBatched, gets, writes :+ (put -> index))
        case ((nonBatched, gets, writes), (delete @ DeleteItem(_, _, _, _, _, _), index)) =>
          (nonBatched, gets, writes :+ (delete -> index))
        case ((nonBatched, gets, writes), (nonGetItem, index))                            =>
          (nonBatched :+ (nonGetItem -> index), gets, writes)
      }

    val indexedBatchGetItem: (BatchGetItem, Chunk[Int]) = gets
      .foldLeft[(BatchGetItem, Chunk[Int])]((BatchGetItem(), Chunk.empty)) {
        case ((batchGetItem, indexes), (getItem, index)) => (batchGetItem + getItem, indexes :+ index)
      }

    val indexedBatchWrite: (BatchWriteItem, Chunk[Int]) = writes
      .foldLeft[(BatchWriteItem, Chunk[Int])]((BatchWriteItem(), Chunk.empty)) {
        case ((batchWriteItem, indexes), (writeItem, index)) => (batchWriteItem + writeItem, indexes :+ index)
      }

    (nonBatched, indexedBatchGetItem, indexedBatchWrite)
  }

  private[dynamodb] def parallelize[A](query: DynamoDBQuery[A]): (Chunk[Constructor[Any]], Chunk[Any] => A) =
    query match {
      case Map(query, mapper) =>
        parallelize(query) match {
          case (constructors, assembler) =>
            (constructors, assembler.andThen(mapper))
        }

      case Zip(left, right)   =>
        val (constructorsLeft, assemblerLeft)   = parallelize(left)
        val (constructorsRight, assemblerRight) = parallelize(right)
        (
          constructorsLeft ++ constructorsRight,
          (results: Chunk[Any]) => {
            val (leftResults, rightResults) = results.splitAt(constructorsLeft.length)
            val left                        = assemblerLeft(leftResults)
            val right                       = assemblerRight(rightResults)
            (left, right).asInstanceOf[A]
          }
        )

      case Succeed(value)     => (Chunk.empty, _ => value.asInstanceOf[A])

      case batchGetItem @ BatchGetItem(_, _, _)                 =>
        (
          Chunk(batchGetItem),
          (results: Chunk[Any]) => {
            println(s"BatchGetItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case batchWriteItem @ BatchWriteItem(_, _, _, _)          =>
        (
          Chunk(batchWriteItem),
          (results: Chunk[Any]) => {
            println(s"BatchWriteItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case getItem @ GetItem(_, _, _, _, _)                     =>
        (
          Chunk(getItem),
          (results: Chunk[Any]) => {
            println(s"GetItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case putItem @ PutItem(_, _, _, _, _, _)                  =>
        (
          Chunk(putItem),
          (results: Chunk[Any]) => {
            println(s"PutItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case updateItem @ UpdateItem(_, _, _, _, _, _, _)         =>
        (
          Chunk(updateItem),
          (results: Chunk[Any]) => {
            println(s"UpdateItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case deleteItem @ DeleteItem(_, _, _, _, _, _)            =>
        (
          Chunk(deleteItem),
          (results: Chunk[Any]) => {
            println(s"DeleteItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case scan @ ScanPage(_, _, _, _, _, _, _, _, _)           =>
        (
          Chunk(scan),
          (results: Chunk[Any]) => {
            println(s"ScanPage results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case scan @ ScanAll(_, _, _, _, _, _, _, _)               =>
        (
          Chunk(scan),
          (results: Chunk[Any]) => {
            println(s"ScanAll results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case query @ QueryPage(_, _, _, _, _, _, _, _, _, _)      =>
        (
          Chunk(query),
          (results: Chunk[Any]) => {
            println(s"QueryPage results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case query @ QueryAll(_, _, _, _, _, _, _, _, _)          =>
        (
          Chunk(query),
          (results: Chunk[Any]) => {
            println(s"QueryAll results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case createTable @ CreateTable(_, _, _, _, _, _, _, _, _) =>
        (
          Chunk(createTable),
          (results: Chunk[Any]) => {
            println(s"Query results=$results")
            results.head.asInstanceOf[A]
          }
        )

    }

}
