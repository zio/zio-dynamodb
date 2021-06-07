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
  QuerySome,
  ScanAll,
  ScanSome,
  UpdateItem
}
import zio.dynamodb.UpdateExpression.Action
import zio.stream.Stream
import zio.{ Chunk, ZIO }

sealed trait DynamoDBQuery[+A] { self =>

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
      ddbExecute(batchWriteItem).as(batchWriteItem.addList.map(_ => ()) zip batchWriteIndexes)

    (indexedNonBatchedResults zipPar indexedGetResults zipPar indexedWriteResults).map {
      case ((nonBatched, batchedGets), batchedWrites) =>
        val combined = (nonBatched ++ batchedGets ++ batchedWrites).sortBy {
          case (_, index) => index
        }.map(_._1)
        assembler(combined)
    }

  }

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
      case q: ScanSome       =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case q: QueryAll       =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case q: QuerySome      =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case m: PutItem        =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case m: UpdateItem     =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case m: DeleteItem     =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[A]]
      case _                 => self // TODO: log a warning
    }

  final def consistency(consistency: ConsistencyMode): DynamoDBQuery[A] =
    self match {
      case g: GetItem   =>
        g.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: ScanAll   =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: ScanSome  =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: QueryAll  =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case q: QuerySome =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[A]]
      case _            => self // TODO: log a warning
    }

  def returns(returnValues: ReturnValues): DynamoDBQuery[A] =
    self match {
      case p: PutItem    =>
        p.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[A]]
      case u: UpdateItem =>
        u.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[A]]
      case d: DeleteItem =>
        d.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[A]]
      case _             => self // TODO: log a warning
    }

  def where(conditionExpression: ConditionExpression): DynamoDBQuery[A] =
    self match {
      case p: PutItem    =>
        p.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[A]]
      case u: UpdateItem =>
        u.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[A]]
      case d: DeleteItem =>
        d.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[A]]
      case _             => self // TODO: log a warning
    }

  def metrics(itemMetrics: ReturnItemCollectionMetrics): DynamoDBQuery[A] =
    self match {
      case p: PutItem    =>
        p.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[A]]
      case u: UpdateItem =>
        u.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[A]]
      case d: DeleteItem =>
        d.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[A]]
      case _             => self // TODO: log a warning
    }

  def startKey(exclusiveStartKey: LastEvaluatedKey): DynamoDBQuery[A] =
    self match {
      case s: ScanSome  => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[A]]
      case s: ScanAll   => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[A]]
      case s: QuerySome => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[A]]
      case s: QueryAll  => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[A]]
      case _            => self // TODO: log a warning
    }

  def filter(filterExpression: FilterExpression): DynamoDBQuery[A] =
    self match {
      case s: ScanSome  => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[A]]
      case s: ScanAll   => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[A]]
      case s: QuerySome => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[A]]
      case s: QueryAll  => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[A]]
      case _            => self // TODO: log a warning
    }

  // TODO: add convenience methods eg `def selectAll: DynamoDBQuery[A]` etc etc
  def select(select: Select): DynamoDBQuery[A] =
    self match {
      case s: ScanSome  => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[A]]
      case s: ScanAll   => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[A]]
      case s: QuerySome => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[A]]
      case s: QueryAll  => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[A]]
      case _            => self // TODO: log a warning
    }

  def whereKey(keyConditionExpression: KeyConditionExpression): DynamoDBQuery[A] =
    self match {
      case s: QuerySome => s.copy(keyConditionExpression = Some(keyConditionExpression)).asInstanceOf[DynamoDBQuery[A]]
      case s: QueryAll  => s.copy(keyConditionExpression = Some(keyConditionExpression)).asInstanceOf[DynamoDBQuery[A]]
      case _            => self // TODO: log a warning
    }

  def sortOrder(ascending: Boolean): DynamoDBQuery[A] =
    self match {
      case s: QuerySome => s.copy(ascending = ascending).asInstanceOf[DynamoDBQuery[A]]
      case s: QueryAll  => s.copy(ascending = ascending).asInstanceOf[DynamoDBQuery[A]]
      case _            => self // TODO: log a warning
    }

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamoDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B])(implicit z: Zippable[A, B]): DynamoDBQuery[z.Out] =
    DynamoDBQuery.Zip[A, B, z.Out](self, that, z)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)

  final def zipWith[B, C](that: DynamoDBQuery[B])(f: (A, B) => C): DynamoDBQuery[C] =
    self.zip(that).map(f.tupled)

}

object DynamoDBQuery {
  import scala.collection.immutable.{ Map => ScalaMap }

  sealed trait Constructor[+A] extends DynamoDBQuery[A]
  sealed trait Write[+A]       extends Constructor[A]

  def succeed[A](a: A): DynamoDBQuery[A] = Succeed(() => a)

  def forEach[A, B](values: Iterable[A])(body: A => DynamoDBQuery[B]): DynamoDBQuery[List[B]] =
    values.foldRight[DynamoDBQuery[List[B]]](succeed(Nil)) {
      case (a, query) => body(a).zipWith(query)(_ :: _)
    }

  def getItem(
    tableName: String,
    key: PrimaryKey,
    projections: ProjectionExpression*
  ): Constructor[Option[Item]] = {
    println(projections)
    GetItem(TableName(tableName), key)
  }

  def putItem(tableName: String, item: Item): Write[Unit] = PutItem(TableName(tableName), item)

  def updateItem(tableName: String, key: PrimaryKey)(action: Action): DynamoDBQuery[Unit] =
    UpdateItem(TableName(tableName), key, UpdateExpression(action))

  def deleteItem(tableName: String, key: PrimaryKey): Write[Unit] = DeleteItem(TableName(tableName), key)

  /**
   * when executed will return a Tuple of {{{(Chunk[Item], LastEvaluatedKey)}}}
   */
  def scanSome(tableName: String, indexName: String, limit: Int, projections: ProjectionExpression*): ScanSome =
    ScanSome(
      TableName(tableName),
      IndexName(indexName),
      limit,
      select = selectOrAll(projections),
      projections = projections.toList
    )

  /**
   * when executed will return a ZStream of Item
   */
  def scanAll(tableName: String, indexName: String, projections: ProjectionExpression*): ScanAll =
    ScanAll(
      TableName(tableName),
      IndexName(indexName),
      select = selectOrAll(projections),
      projections = projections.toList
    )

  /**
   * when executed will return a Tuple of {{{(Chunk[Item], LastEvaluatedKey)}}}
   */
  def querySome(tableName: String, indexName: String, limit: Int, projections: ProjectionExpression*): QuerySome =
    QuerySome(
      TableName(tableName),
      IndexName(indexName),
      limit,
      select = selectOrAll(projections),
      projections = projections.toList
    )

  /**
   * when executed will return a ZStream of Item
   */
  def queryAll(tableName: String, indexName: String, projections: ProjectionExpression*): QueryAll =
    QueryAll(
      TableName(tableName),
      IndexName(indexName),
      select = selectOrAll(projections),
      projections = projections.toList
    )

  def createTable(
    tableName: String,
    keySchema: KeySchema,
    attributeDefinitions: NonEmptySet[AttributeDefinition],
    billingMode: BillingMode = BillingMode.Provisioned,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty,
    provisionedThroughput: Option[ProvisionedThroughput] = None,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty
  ): CreateTable =
    CreateTable(
      TableName(tableName),
      keySchema,
      attributeDefinitions,
      billingMode,
      globalSecondaryIndexes,
      localSecondaryIndexes,
      provisionedThroughput,
      sseSpecification,
      tags
    )

  private def selectOrAll(projections: Seq[ProjectionExpression]): Option[Select] =
    Some(if (projections.isEmpty) Select.AllAttributes else Select.SpecificAttributes)

  private[dynamodb] final case class Succeed[A](value: () => A) extends Constructor[A]

  private[dynamodb] final case class GetItem(
    tableName: TableName,
    key: PrimaryKey,
    projections: List[ProjectionExpression] =
      List.empty, // If no attribute names are specified, then all attributes are returned
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends Constructor[Option[Item]]

  private[dynamodb] final case class BatchGetItem(
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
          val responsesForTable: Set[Item] = response.responses.getOrElse(getItem.tableName, Set.empty[Item])
          val found: Option[Item]          = responsesForTable.find { item =>
            getItem.key.map.toSet.subsetOf(item.map.toSet)
          }

          found.fold(chunk :+ None)(item => chunk :+ Some(item))
      }

      chunk
    }

  }
  private[dynamodb] object BatchGetItem {
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

  private[dynamodb] final case class BatchWriteItem(
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
  private[dynamodb] object BatchWriteItem {
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
  private[dynamodb] final case class ScanSome(
    tableName: TableName,
    indexName: IndexName,
    limit: Int,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                         // if ProjectExpression supplied then only valid value is SpecificAttributes
  ) extends Constructor[(Chunk[Item], LastEvaluatedKey)]

  private[dynamodb] final case class QuerySome(
    tableName: TableName,
    indexName: IndexName,
    limit: Int,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    keyConditionExpression: Option[KeyConditionExpression] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                        // if ProjectExpression supplied then only valid value is SpecificAttributes
    ascending: Boolean = true
  ) extends Constructor[(Chunk[Item], LastEvaluatedKey)]

  private[dynamodb] final case class ScanAll(
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

  private[dynamodb] final case class QueryAll(
    tableName: TableName,
    indexName: IndexName,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                               // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression] = None,
    keyConditionExpression: Option[KeyConditionExpression] = None,
    projections: List[ProjectionExpression] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                        // if ProjectExpression supplied then only valid value is SpecificAttributes
    ascending: Boolean = true
  ) extends Constructor[Stream[Exception, Item]]

  private[dynamodb] final case class PutItem(
    tableName: TableName,
    item: Item,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None // PutItem does not recognize any values other than NONE or ALL_OLD.
  ) extends Write[Unit] // TODO: model response

  private[dynamodb] final case class UpdateItem(
    tableName: TableName,
    key: PrimaryKey,
    updateExpression: UpdateExpression,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None
  ) extends Constructor[Unit] // TODO: model response

  private[dynamodb] final case class DeleteItem(
    tableName: TableName,
    key: PrimaryKey,
    conditionExpression: Option[ConditionExpression] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues =
      ReturnValues.None // DeleteItem does not recognize any values other than NONE or ALL_OLD.
  ) extends Write[Unit] // TODO: model response

  private[dynamodb] final case class CreateTable(
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

  private[dynamodb] final case class Zip[A, B, C](
    left: DynamoDBQuery[A],
    right: DynamoDBQuery[B],
    zippable: Zippable.Out[A, B, C]
  )                                                                                     extends DynamoDBQuery[C]
  private[dynamodb] final case class Map[A, B](query: DynamoDBQuery[A], mapper: A => B) extends DynamoDBQuery[B]

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)

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

      case zip @ Zip(_, _, _) =>
        val (constructorsLeft, assemblerLeft)   = parallelize(zip.left)
        val (constructorsRight, assemblerRight) = parallelize(zip.right)
        (
          constructorsLeft ++ constructorsRight,
          (results: Chunk[Any]) => {
            val (leftResults, rightResults) = results.splitAt(constructorsLeft.length)
            val left                        = assemblerLeft(leftResults)
            val right                       = assemblerRight(rightResults)
            zip.zippable.zip(left, right)
          }
        )

      case Succeed(value)     => (Chunk.empty, _ => value())

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

      case scan @ ScanSome(_, _, _, _, _, _, _, _, _)           =>
        (
          Chunk(scan),
          (results: Chunk[Any]) => {
            println(s"ScanSome results=$results")
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

      case query @ QuerySome(_, _, _, _, _, _, _, _, _, _, _)   =>
        (
          Chunk(query),
          (results: Chunk[Any]) => {
            println(s"QuerySome results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case query @ QueryAll(_, _, _, _, _, _, _, _, _, _)       =>
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
