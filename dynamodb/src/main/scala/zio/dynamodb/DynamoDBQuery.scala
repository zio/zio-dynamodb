package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableItem
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.{ Delete, Put }
import zio.dynamodb.DynamoDBQuery.{ batched, parallelize }
import zio.stream.ZStream
import zio.{ Chunk, ZIO }

sealed trait DynamoDBQuery[+A] { self =>
  final def <*[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = zipLeft(that)

  final def *>[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = zipRight(that)

  final def <*>[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = self zip that

  def execute: ZIO[DynamoDBExecutor, Exception, A] = {
    val (constructors, assembler)                                                                   = parallelize(self)
    val (indexedConstructors, (batchGetItem, batchGetIndexes), (batchWriteItem, batchWriteIndexes)) =
      batched(constructors)

    for {
      indexedNonGetResponses <- ZIO.foreach(indexedConstructors) {
                                  case (constructor, index) =>
                                    ddbExecute(constructor).map(result => (result, index))
                                }
      indexedGetResponses    <-
        ddbExecute(batchGetItem).map(resp => batchGetItem.toGetItemResponses(resp) zip batchGetIndexes)
      indexedWriteResponses  <-
        // TODO: think about mapping return values from writes
        ddbExecute(batchWriteItem).map(_ => batchWriteItem.addList.map(_ => ()) zip batchWriteIndexes)
      combined                = (indexedNonGetResponses ++ indexedGetResponses ++ indexedWriteResponses).sortBy {
                                  case (_, index) => index
                                }.map(_._1)
      result                  = assembler(combined)
    } yield result
  }

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamoDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = DynamoDBQuery.Zip(self, that)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)

}

object DynamoDBQuery {
  import scala.collection.immutable.{ Map => ScalaMap }

  sealed trait Constructor[+A] extends DynamoDBQuery[A]
  sealed trait Write[+A]       extends Constructor[A]

  final case class Succeed[A](value: () => A) extends Constructor[A]

  final case class GetItem(
    key: PrimaryKey,
    tableName: TableName,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    projections: List[ProjectionExpression] =
      List.empty, // If no attribute names are specified, then all attributes are returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends Constructor[Option[Item]]

  // TODO: should this be publicly visible?
  final case class BatchGetItem(
    requestItems: MapOfSet[TableName, BatchGetItem.TableItem] = MapOfSet.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    addList: Chunk[GetItem] = Chunk.empty // track order of added GetItems for later unpacking
  ) extends Constructor[BatchGetItem.Response] { self =>

    def +(getItem: GetItem): BatchGetItem =
      BatchGetItem(
        self.requestItems + ((getItem.tableName, TableItem(getItem.key, getItem.projections))),
        self.capacity,
        self.addList :+ getItem
      )

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
    final case class TableItem(
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

    def ++(that: BatchWriteItem): BatchWriteItem =
      BatchWriteItem(self.requestItems ++ that.requestItems, self.capacity, self.itemMetrics)
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
  ) extends Write[Unit] // TODO: model response

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

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)

//  private def batchGets(
//    tuple: (Chunk[Constructor[Any]], Chunk[Any] => Any)
//  ): (Chunk[Constructor[Any]], Chunk[Any] => Any) =
//    tuple match {
//      case (constructors, assembler) =>
//        type IndexedConstructor = (Constructor[Any], Int)
//        type IndexedGetItem     = (GetItem, Int)
//        // partion into gets/non gets
//        val (nonGets, gets) =
//          constructors.zipWithIndex.foldLeft[(Chunk[IndexedConstructor], Chunk[IndexedGetItem])]((Chunk.empty, Chunk.empty)) {
//            case ((nonGets, gets), (y: GetItem, index)) => (nonGets, gets :+ ((y, index)))
//            case ((nonGets, gets), (y, index))          => (nonGets :+ ((y, index)), gets)
//          }
//        /*
//
//         */
//        val x = BatchGetItem(MapOfSet.empty, )
//    }

  private[dynamodb] def batched(
    constructors: Chunk[Constructor[Any]]
  ): (Chunk[(Constructor[Any], Int)], (BatchGetItem, Chunk[Int]), (BatchWriteItem, Chunk[Int])) = {
    type IndexedConstructor = (Constructor[Any], Int)
    type IndexedGetItem     = (GetItem, Int)
    type IndexedWriteItem   = (Write[Unit], Int)

    // partition into nonBatched/batched gets/batched writes
    val (nonBatched, gets, writes) =
      constructors.zipWithIndex.foldLeft[(Chunk[IndexedConstructor], Chunk[IndexedGetItem], Chunk[IndexedWriteItem])](
        (Chunk.empty, Chunk.empty, Chunk.empty)
      ) {
        case ((nonBatched, gets, writes), (get @ GetItem(_, _, _, _, _), index))          =>
          (nonBatched, gets :+ ((get, index)), writes)
        case ((nonBatched, gets, writes), (put @ PutItem(_, _, _, _, _, _), index))       =>
          (nonBatched, gets, writes :+ ((put, index)))
        case ((nonBatched, gets, writes), (delete @ DeleteItem(_, _, _, _, _, _), index)) =>
          (nonBatched, gets, writes :+ ((delete, index)))
        case ((nonBatched, gets, writes), (nonGetItem, index))                            =>
          (nonBatched :+ ((nonGetItem, index)), gets, writes)
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

      case scan @ Scan(_, _, _, _, _, _, _, _, _)               =>
        (
          Chunk(scan),
          (results: Chunk[Any]) => {
            println(s"Scan results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case query @ Query(_, _, _, _, _, _, _, _, _, _)          =>
        (
          Chunk(query),
          (results: Chunk[Any]) => {
            println(s"Query results=$results")
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
