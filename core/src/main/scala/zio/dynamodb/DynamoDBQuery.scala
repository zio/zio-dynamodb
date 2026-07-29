package zio.dynamodb

import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.{ Delete, Put }
import zio.dynamodb.DynamoDBQuery.ZipPar
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.Utils.ListUtils
import zio.dynamodb.proofs.{ CanFilter, CanWhere }

import scala.+:

final case class DummyIO[+A](unsafeRun: () => A)
object DummyIO {
  def succeed[A](a: => A): DummyIO[A] = DummyIO(() => a)
}

sealed trait DynamoDBQuery[-In, +Out] { self =>

  final def map[B](f: Out => B): DynamoDBQuery[In, B] = DynamoDBQuery.Map(self, f)

  /** Runs `self` and `that` in parallel and combines their results. */
  final def zipPar[In1 <: In, B](that: DynamoDBQuery[In1, B])(implicit
    z: Zippable[Out, B]
  ): DynamoDBQuery[In1, z.Out] =
    DynamoDBQuery.ZipPar[Out, B, z.Out](self, that, z)

  final def zipParLeft[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, Out] =
    (self zipPar that).map(_._1)

  final def zipParRight[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, B] =
    (self zipPar that).map(_._2)

  final def zipParWith[In1 <: In, B, C](that: DynamoDBQuery[In1, B])(
    f: (Out, B) => C
  ): DynamoDBQuery[In1, C] =
    (self zipPar that).map(f.tupled)

  def where[B](conditionExpression: ConditionExpression[B])(implicit ev: CanWhere[B, Out]): DynamoDBQuery[In, Out] = {
    val _ = ev
    self match {
      case zp @ ZipPar(left, right, zippable)     =>
        ZipPar(
          left.where(conditionExpression.asInstanceOf[ConditionExpression[zp.Left]]),
          right.where(conditionExpression.asInstanceOf[ConditionExpression[zp.Right]]),
          zippable
        )
      case map @ DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.where(conditionExpression.asInstanceOf[ConditionExpression[map.Old]]), mapper)
      case ab @ DynamoDBQuery.Absolve(query)      =>
        DynamoDBQuery.Absolve(query.where(conditionExpression.asInstanceOf[ConditionExpression[ab.Old]]))
      case p: DynamoDBQuery.PutItem               =>
        p.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem            =>
        u.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem            =>
        d.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                      => self
    }
  }

  /**
   * Adds a KeyConditionExpr to a DynamoDBQuery. Example:
   * {{{
   * // high level type safe API where "email" and "subject" keys are defined using ProjectionExpression.accessors[Student]
   * val newQuery = query.whereKey(email.partitionKey === "avi@gmail.com" && subject.sortKey === "maths")
   *
   * // low level API
   * val newQuery = query.whereKey($("email").partitionKey === "avi@gmail.com" && $("subject").sortKey === "maths")
   * }}}
   */
  def whereKey[From](keyConditionExpression: KeyConditionExpr[From]): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(left.whereKey(keyConditionExpression), right.whereKey(keyConditionExpression), zippable)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.whereKey(keyConditionExpression), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.whereKey(keyConditionExpression))

      case s: DynamoDBQuery.QuerySome =>
        s.copy(keyConditionExpr = Some(keyConditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                          => self
    }

  final def returnValuesOnConditionCheckFailure(
    rv: ReturnValuesOnConditionCheckFailure
  ): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(
          left.returnValuesOnConditionCheckFailure(rv),
          right.returnValuesOnConditionCheckFailure(rv),
          zippable
        )
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.returnValuesOnConditionCheckFailure(rv), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.returnValuesOnConditionCheckFailure(rv))
      case p: DynamoDBQuery.PutItem         =>
        p.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem      =>
        u.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem      =>
        d.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case c: DynamoDBQuery.ConditionCheck  =>
        c.copy(returnValuesOnConditionCheckFailure = Some(rv)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => self
    }

  final def withClientRequestToken(token: String): DynamoDBQuery[In, Out] =
    self match {
      case tw: DynamoDBQuery.TransactWriteItems =>
        tw.copy(clientRequestToken = Some(token)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                    => self
    }

  final def withRetryPolicy(policy: RetryPolicy): DynamoDBQuery[In, Out] =
    self match {
      case zp @ ZipPar(left, right, zippable)     =>
        ZipPar(left.withRetryPolicy(policy), right.withRetryPolicy(policy), zippable)
      case map @ DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.withRetryPolicy(policy), mapper)
      case ab @ DynamoDBQuery.Absolve(query)      =>
        DynamoDBQuery.Absolve(query.withRetryPolicy(policy))
      case bw: DynamoDBQuery.BatchWriteItem       => bw.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case bg: DynamoDBQuery.BatchGetItem         => bg.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case p: DynamoDBQuery.PutItem               => p.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem            => d.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case g: DynamoDBQuery.GetItem               => g.copy(retryPolicy = Some(policy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                      => self
    }

  /**
   * Filter a Scan or a Query
   */
  def filter[B](filterExpression: FilterExpression[B])(implicit ev: CanFilter[B, Out]): DynamoDBQuery[In, Out] = {
    val _ = ev
    self match {
      case zp @ ZipPar(left, right, zippable)     =>
        ZipPar(
          left.filter(filterExpression.asInstanceOf[FilterExpression[zp.Left]]),
          right.filter(filterExpression.asInstanceOf[FilterExpression[zp.Right]]),
          zippable
        )
      case map @ DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.filter(filterExpression.asInstanceOf[FilterExpression[map.Old]]), mapper)
      case ab @ DynamoDBQuery.Absolve(query)      =>
        DynamoDBQuery.Absolve(query.filter(filterExpression.asInstanceOf[FilterExpression[ab.Old]]))

      case s: DynamoDBQuery.ScanSome  =>
        s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.QuerySome =>
        s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                          => self
    }
  }

  final def indexName(indexName: String): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    => ZipPar(left.indexName(indexName), right.indexName(indexName), zippable)
      case DynamoDBQuery.Map(query, mapper) => DynamoDBQuery.Map(query.indexName(indexName), mapper)
      case DynamoDBQuery.Absolve(query)     => DynamoDBQuery.Absolve(query.indexName(indexName))
      case q: DynamoDBQuery.ScanSome        =>
        q.copy(indexName = Some(indexName)).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: DynamoDBQuery.QuerySome       =>
        q.copy(indexName = Some(indexName)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => self
    }

  final def capacity(capacity: ReturnConsumedCapacity): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)        =>
        ZipPar(left.capacity(capacity), right.capacity(capacity), zippable)
      case DynamoDBQuery.Map(query, mapper)     =>
        DynamoDBQuery.Map(query.capacity(capacity), mapper)
      case DynamoDBQuery.Absolve(query)         =>
        DynamoDBQuery.Absolve(query.capacity(capacity))
      case g: DynamoDBQuery.GetItem             =>
        g.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case p: DynamoDBQuery.PutItem             =>
        p.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem          =>
        u.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem          =>
        d.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.ScanSome            =>
        s.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: DynamoDBQuery.QuerySome           =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case bg: DynamoDBQuery.BatchGetItem       =>
        bg.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case bw: DynamoDBQuery.BatchWriteItem     =>
        bw.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case tg: DynamoDBQuery.TransactGetItems   =>
        tg.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case tw: DynamoDBQuery.TransactWriteItems =>
        tw.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                    => self
    }

  final def consistency(consistency: ConsistencyMode): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(left.consistency(consistency), right.consistency(consistency), zippable)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.consistency(consistency), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.consistency(consistency))
      case g: DynamoDBQuery.GetItem         =>
        g.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.ScanSome        =>
        s.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: DynamoDBQuery.QuerySome       =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => self
    }

  final def returns(returnValues: ReturnValues): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(left.returns(returnValues), right.returns(returnValues), zippable)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.returns(returnValues), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.returns(returnValues))
      case p: DynamoDBQuery.PutItem         =>
        p.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem      =>
        u.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem      =>
        d.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => self
    }

  final def metrics(itemMetrics: ReturnItemCollectionMetrics): DynamoDBQuery[In, Out] =
    self match {
      case ZipPar(left, right, zippable)    =>
        ZipPar(left.metrics(itemMetrics), right.metrics(itemMetrics), zippable)
      case DynamoDBQuery.Map(query, mapper) =>
        DynamoDBQuery.Map(query.metrics(itemMetrics), mapper)
      case DynamoDBQuery.Absolve(query)     =>
        DynamoDBQuery.Absolve(query.metrics(itemMetrics))
      case p: DynamoDBQuery.PutItem         =>
        p.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: DynamoDBQuery.UpdateItem      =>
        u.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DynamoDBQuery.DeleteItem      =>
        d.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case bw: DynamoDBQuery.BatchWriteItem =>
        bw.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => self
    }

  def startKey(exclusiveStartKey: LastEvaluatedKey): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(left.startKey(exclusiveStartKey), right.startKey(exclusiveStartKey), zippable)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.startKey(exclusiveStartKey), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.startKey(exclusiveStartKey))
      case s: DynamoDBQuery.ScanSome                   =>
        s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.QuerySome                  =>
        s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => self
    }

  def sortOrder(ascending: Boolean): DynamoDBQuery[In, Out] =
    self match {
      case q: DynamoDBQuery.QuerySome       => q.copy(ascending = ascending).asInstanceOf[DynamoDBQuery[In, Out]]
      case DynamoDBQuery.Map(query, mapper) => DynamoDBQuery.Map(query.sortOrder(ascending), mapper)
      case DynamoDBQuery.Absolve(query)     => DynamoDBQuery.Absolve(query.sortOrder(ascending))
      case _                                => self
    }

  def segment(index: Int, total: Int): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.Map(query, mapper) => DynamoDBQuery.Map(query.segment(index, total), mapper)
      case DynamoDBQuery.Absolve(query)     => DynamoDBQuery.Absolve(query.segment(index, total))
      case s: DynamoDBQuery.ScanSome        =>
        s.copy(segment = index, totalSegments = total).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                => self
    }

  def gsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType,
    readCapacityUnit: Long,
    writeCapacityUnit: Long
  ): DynamoDBQuery[In, Out] =

    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(
          left.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit),
          right.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit),
          zippable
        )
      case DynamoDBQuery.Map(query, mapper)            =>
        DynamoDBQuery.Map(query.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit), mapper)
      case DynamoDBQuery.Absolve(query)                =>
        DynamoDBQuery.Absolve(query.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit))
      case s: DynamoDBQuery.CreateTable                =>
        s.copy(globalSecondaryIndexes =
          s.globalSecondaryIndexes + GlobalSecondaryIndex(
            indexName,
            keySchema,
            projection,
            Some(ProvisionedThroughput(readCapacityUnit, writeCapacityUnit))
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => self
    }

  def gsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType
  ): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(
          left.gsi(indexName, keySchema, projection),
          right.gsi(indexName, keySchema, projection),
          zippable
        )
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.gsi(indexName, keySchema, projection), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.gsi(indexName, keySchema, projection))
      case s: DynamoDBQuery.CreateTable                =>
        s.copy(globalSecondaryIndexes =
          s.globalSecondaryIndexes + GlobalSecondaryIndex(
            indexName,
            keySchema,
            projection,
            None
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => self
    }

  def lsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType = ProjectionType.All
  ): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(
          left.lsi(indexName, keySchema, projection),
          right.lsi(indexName, keySchema, projection),
          zippable
        )
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.lsi(indexName, keySchema, projection), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.lsi(indexName, keySchema, projection))
      case s: DynamoDBQuery.CreateTable                =>
        s.copy(localSecondaryIndexes =
          s.localSecondaryIndexes + LocalSecondaryIndex(
            indexName,
            keySchema,
            projection
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => self
    }

  def selectAllAttributes: DynamoDBQuery[In, Out]          = select(Select.AllAttributes)
  def selectAllProjectedAttributes: DynamoDBQuery[In, Out] = select(Select.AllProjectedAttributes)
  def selectSpecificAttributes: DynamoDBQuery[In, Out]     = select(Select.SpecificAttributes)
  def selectCount: DynamoDBQuery[In, Out]                  = select(Select.Count)

  private def select(select: Select): DynamoDBQuery[In, Out] =
    self match {
      case DynamoDBQuery.ZipPar(left, right, zippable) =>
        DynamoDBQuery.ZipPar(left.select(select), right.select(select), zippable)
      case DynamoDBQuery.Map(query, mapper)            => DynamoDBQuery.Map(query.select(select), mapper)
      case DynamoDBQuery.Absolve(query)                => DynamoDBQuery.Absolve(query.select(select))
      case s: DynamoDBQuery.ScanSome                   => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: DynamoDBQuery.QuerySome                  => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                           => self
    }

}

object DynamoDBQuery {
  import scala.collection.immutable.{ Map => ScalaMap, Set => ScalaSet }

  def succeed[A](a: => A): DynamoDBQuery[Any, A] = Succeed(() => a)

  def fail(e: => DynamoDBError): DynamoDBQuery[Any, Nothing] = Fail(() => e)

  private[dynamodb] final case class ZipPar[A, B, C](
    left: DynamoDBQuery[_, A],
    right: DynamoDBQuery[_, B],
    zippable: Zippable.Out[A, B, C]
  ) extends DynamoDBQuery[Any, C] {
    type Left  = A
    type Right = B
  }

  private[dynamodb] final case class Map[A, B](query: DynamoDBQuery[_, A], mapper: A => B)
      extends DynamoDBQuery[Any, B] {
    type Old = A
  }

  private[dynamodb] final case class Fail(error: () => DynamoDBError) extends Constructor[Any, Nothing]

  private[dynamodb] final case class Absolve[A, B](query: DynamoDBQuery[A, Either[ItemError, B]])
      extends DynamoDBQuery[A, B] {
    type Old = Either[ItemError, B]
  }

  def apply[A](a: => A): DynamoDBQuery[Any, A] = Succeed(() => a)

  private[dynamodb] final case class Succeed[A](value: () => A) extends Constructor[Any, A]

  def batchGetItem[In, A, B](values: Iterable[A])(body: A => DynamoDBQuery[In, B]): DynamoDBQuery.BatchGetItem =
    values.foldLeft(BatchGetItem()) { (batch, a) =>
      batch + body(a).asInstanceOf[GetItem]
    }

  def batchWriteItem[In, A, B](values: Iterable[A])(body: A => Write[In, B]): DynamoDBQuery.BatchWriteItem =
    values.foldLeft(BatchWriteItem()) { (batch, a) =>
      batch + body(a).asInstanceOf[Write[Any, B]]
    }

  sealed trait Constructor[-In, +A] extends DynamoDBQuery[In, A]

  sealed trait Write[-In, +A] extends Constructor[In, A]

  final case class GetItem(
    tableName: String,
    key: PrimaryKey,
    projections: List[ProjectionExpression[_, _]] =
      List.empty, // If no attribute names are specified, then all attributes are returned,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    retryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, Option[Item]]

  private[dynamodb] final case class PutItem(
    tableName: String,
    item: Item,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None, // PutItem does not recognize any values other than NONE or ALL_OLD.
    retryPolicy: Option[RetryPolicy] = None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None
  ) extends Write[Any, Option[Item]]

  private[dynamodb] final case class UpdateItem(
    tableName: String,
    key: PrimaryKey,
    updateExpression: UpdateExpression[_],
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None
  ) extends Constructor[Any, Option[Item]]

  private[dynamodb] final case class DeleteItem(
    tableName: String,
    key: PrimaryKey,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues =
      ReturnValues.None, // DeleteItem does not recognize any values other than NONE or ALL_OLD.
    retryPolicy: Option[RetryPolicy] = None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None
  ) extends Write[Any, Option[Item]]

  private[dynamodb] final case class ConditionCheck(
    tableName: String,
    key: PrimaryKey,
    conditionExpression: ConditionExpression[_],
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    returnValuesOnConditionCheckFailure: Option[ReturnValuesOnConditionCheckFailure] = None
  ) extends Constructor[Any, Unit]

  private[dynamodb] final case class TransactGetItems(
    getItems: Chunk[GetItem],
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  ) extends Constructor[Any, Chunk[Option[Item]]]

  private[dynamodb] final case class TransactWriteItems(
    writeItems: Chunk[DynamoDBQuery[Any, Any]],
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    clientRequestToken: Option[String] = None
  ) extends Constructor[Any, Unit]

  private[dynamodb] final case class ScanSome(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                              // if ProjectExpression supplied then only valid value is SpecificAttributes
    segment: Int = 0,
    totalSegments: Int = 1
  ) extends Constructor[Any, Page[Item]]

  private[dynamodb] final case class QuerySome(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    keyConditionExpr: Option[KeyConditionExpr[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                              // if ProjectExpression supplied then only valid value is SpecificAttributes
    ascending: Boolean = true
  ) extends Constructor[Any, Page[Item]]

  private[dynamodb] final case class CreateTable(
    tableName: String,
    keySchema: KeySchema,
    attributeDefinitions: NonEmptySet[AttributeDefinition],
    billingMode: BillingMode,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty // you can have up to 50 tags
  ) extends Constructor[Any, Unit]
  private[dynamodb] final case class DeleteTable(
    tableName: String
  ) extends Constructor[Any, Unit]
  private[dynamodb] final case class DescribeTable(
    tableName: String
  ) extends Constructor[Any, DescribeTableResponse]
  // TODO: (adam) Add more fields here, this was for some basic testing initially
  final case class DescribeTableResponse(
    tableArn: String,
    tableStatus: TableStatus,
    tableSizeBytes: Long,
    itemCount: Long
  ) {
    override def toString: String =
      s"tableArn: $tableArn, tableStatus: $tableStatus, tableSizeBytes: $tableSizeBytes, itemCount: $itemCount"
  }
  sealed trait TableStatus
  object TableStatus {
    case object Creating                          extends TableStatus
    case object Updating                          extends TableStatus
    case object Deleting                          extends TableStatus
    case object Active                            extends TableStatus
    case object InaccessibleEncryptionCredentials extends TableStatus
    case object Archiving                         extends TableStatus
    case object Archived                          extends TableStatus
    case object ReplicationNotAuthorized          extends TableStatus
    case object unknownToSdkVersion               extends TableStatus
  }

  private[dynamodb] final case class BatchGetItem(
    requestItems: ScalaMap[String, BatchGetItem.TableGet] = ScalaMap.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    private[dynamodb] val orderedGetItems: Chunk[GetItem] =
      Chunk.empty, // track order of added GetItems for later unpacking
    retryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, BatchGetItem.Response] { self =>

    def +(getItem: GetItem): BatchGetItem = {
      val tableName                                                     = getItem.tableName
      val key                                                           = getItem.key
      val projectionExpressionSet: ScalaSet[ProjectionExpression[_, _]] = getItem.projections.toSet
      val newEntry: (String, BatchGetItem.TableGet)                     =
        self.requestItems
          .get(tableName)
          .fold((tableName, BatchGetItem.TableGet(ScalaSet(key), getItem.projections.toSet)))(t =>
            (
              tableName,
              BatchGetItem.TableGet(t.keysSet + key, t.projectionExpressionSet ++ projectionExpressionSet)
            )
          )
      BatchGetItem(
        self.requestItems + newEntry,
        self.capacity,
        self.orderedGetItems :+ getItem,
        self.retryPolicy.orElse(getItem.retryPolicy) // inherit retry policy from GetItem if not set
      )
    }

    def addAll(entries: GetItem*): BatchGetItem =
      entries.foldLeft(self) { case (batch, getItem) =>
        batch + getItem
      }

    /*
     for each added GetItem, check it's key exists in the response and create a corresponding Optional Item value
     */
    def toGetItemResponses(response: BatchGetItem.Response): Chunk[Option[Item]] = {
      val chunk: Chunk[Option[Item]] = orderedGetItems.foldLeft[Chunk[Option[Item]]](Chunk.empty) {
        case (chunk, getItem) =>
          val responsesForTable: Set[Item] = response.responses.getOrElse(getItem.tableName, Set.empty[Item])
          // What if the projection expression for responsesForTable doesn't include the primaryKey?
          // Shouldn't the responseForTable have only the requested item?
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
      keysSet: ScalaSet[PrimaryKey],
      projectionExpressionSet: ScalaSet[ProjectionExpression[_, _]]
    )
    final case class Response(
      // Note - if a requested item does not exist, it is not returned in the result
      responses: MapOfSet[String, Item] = MapOfSet.empty,
      unprocessedKeys: ScalaMap[String, TableGet] = ScalaMap.empty
    )
  }

  private[dynamodb] final case class BatchWriteItem(
    requestItems: MapOfSet[String, BatchWriteItem.Write] = MapOfSet.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    addList: Chunk[BatchWriteItem.Write] = Chunk.empty,
    retryPolicy: Option[RetryPolicy] = None
  ) extends Constructor[Any, BatchWriteItem.Response] { self =>
    def +[A](writeItem: Write[Any, A]): BatchWriteItem =
      writeItem match {
        case putItem @ PutItem(_, _, _, _, _, _, _, _)       =>
          BatchWriteItem(
            self.requestItems + ((putItem.tableName, Put(putItem.item))),
            self.capacity,
            self.itemMetrics,
            self.addList :+ Put(putItem.item),
            self.retryPolicy.orElse(putItem.retryPolicy) // inherit retry policy from PutItem if not set
          )
        case deleteItem @ DeleteItem(_, _, _, _, _, _, _, _) =>
          BatchWriteItem(
            self.requestItems + ((deleteItem.tableName, Delete(deleteItem.key))),
            self.capacity,
            self.itemMetrics,
            self.addList :+ Delete(deleteItem.key),
            self.retryPolicy.orElse(deleteItem.retryPolicy) // inherit retry policy from DeleteItem if not set
          )
      }

    def addAll[A](entries: Write[Any, A]*): BatchWriteItem =
      entries.foldLeft(self) { case (batch, write) =>
        batch + write
      }
  }

  private[dynamodb] object BatchWriteItem {
    sealed trait Write
    final case class Delete(key: PrimaryKey) extends Write
    final case class Put(item: Item)         extends Write

    final case class Response(
      unprocessedItems: Option[MapOfSet[String, BatchWriteItem.Write]]
    )

  }

  def putItem(
    tableName: String,
    item: Item,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None, // PutItem does not recognize any values other than NONE or ALL_OLD.
    retryPolicy: Option[RetryPolicy] = None
  ): Write[Any, Option[Item]] =
    PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues, retryPolicy)

  def updateItem[A](tableName: String, key: PrimaryKey)(action: Action[A]): DynamoDBQuery[A, Option[Item]] =
    UpdateItem(tableName, key, UpdateExpression(action))

  def updateItem(
    tableName: String,
    key: PrimaryKey,
    updateExpression: UpdateExpression[_],
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None
  ): DynamoDBQuery[Any, Option[Item]] =
    UpdateItem(tableName, key, updateExpression, conditionExpression, capacity, itemMetrics, returnValues)

  def getItem(
    tableName: String,
    key: PrimaryKey,
    projections: ProjectionExpression[_, _]*
  ): DynamoDBQuery[Any, Option[Item]] =
    GetItem(tableName, key, projections.toList)

  def deleteItem(
    tableName: String,
    key: PrimaryKey,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None,
    retryPolicy: Option[RetryPolicy] = None
  ): Write[Any, Option[Item]] =
    DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues, retryPolicy)

  def createTable(
    tableName: String,
    keySchema: KeySchema,
    attributeDefinitions: NonEmptySet[AttributeDefinition],
    billingMode: BillingMode,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty
  ): DynamoDBQuery[Any, Unit] =
    CreateTable(
      tableName,
      keySchema,
      attributeDefinitions,
      billingMode,
      globalSecondaryIndexes,
      localSecondaryIndexes,
      sseSpecification,
      tags
    )

  def deleteTable(tableName: String): DynamoDBQuery[Any, Unit] =
    DeleteTable(tableName)

  def describeTable(tableName: String): DynamoDBQuery[Any, DescribeTableResponse] =
    DescribeTable(tableName)

  def querySome(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey = None,
    ascending: Boolean = true
  ): DynamoDBQuery[Any, Page[Item]] =
    QuerySome(tableName, limit, indexName, consistency, exclusiveStartKey, ascending = ascending)

  def scanSome(
    tableName: String,
    limit: Int,
    indexName: Option[String] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey = None,
    filterExpression: Option[FilterExpression[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None
  ): DynamoDBQuery[Any, Page[Item]] =
    ScanSome(
      tableName,
      limit,
      indexName,
      consistency,
      exclusiveStartKey,
      filterExpression,
      projections,
      capacity,
      select
    )

  def transactGetItems(items: GetItem*): DynamoDBQuery[Any, Chunk[Option[Item]]] =
    TransactGetItems(Chunk.fromIterable(items))

  def transactWriteItems(items: DynamoDBQuery[Any, _]*): DynamoDBQuery[Any, Unit] =
    TransactWriteItems(Chunk.fromIterable(items.map(_.asInstanceOf[DynamoDBQuery[Any, Any]])))

  def conditionCheck(
    tableName: String,
    primaryKey: PrimaryKey
  )(conditionExpression: ConditionExpression[_]): ConditionCheck =
    ConditionCheck(tableName, primaryKey, conditionExpression)

}
