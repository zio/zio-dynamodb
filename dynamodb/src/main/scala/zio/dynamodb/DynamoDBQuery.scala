package zio.dynamodb

import zio.dynamodb.DynamoDBError.BatchError
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.ValueNotFound
import zio.dynamodb.proofs.{ CanFilter, CanWhere }
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.{ Delete, Put }
import zio.dynamodb.DynamoDBQuery.{
  batched,
  parallelize,
  Absolve,
  BatchGetItem,
  BatchWriteItem,
  CreateTable,
  DeleteItem,
  GetItem,
  Map,
  PutItem,
  QueryAll,
  QuerySome,
  ScanAll,
  ScanSome,
  Transaction,
  UpdateItem,
  Zip
}
import zio.dynamodb.UpdateExpression.Action
import zio.prelude.ForEachOps
import zio.schema.Schema
import zio.stream.Stream
import zio.{ Chunk, Schedule, ZIO }
import scala.annotation.nowarn

/**
 * A description of one or more DynamoDB requests that produces an `Out` when it runs. The constructors in the
 * `DynamoDBQuery` companion object build it, for the High Level API (`get`, `put`, `update`, `deleteFrom`, `scanAll`,
 * ...), which encodes and decodes a model type with its `Schema`, and for the Low Level API (`getItem`, `putItem`,
 * ...), which works with `Item`s. `In` is the model type of a High Level query (`Any` for the Low Level API), so that
 * the compiler rejects, for example, a filter on the fields of another model.
 *
 * Nothing runs until `execute` turns the query into a ZIO effect that needs a `DynamoDBExecutor`. Queries combined
 * with `zip` and its variants, or with `DynamoDBQuery.batch`, run together: gets and writes that DynamoDB can batch
 * are sent as `BatchGetItem` and `BatchWriteItem` requests, and the other queries run in parallel. The option setters
 * (`where`, `filter`, `capacity`, `returns`, ...) apply to every query inside a composite query and leave the queries
 * they do not apply to unchanged.
 * {{{
 * import zio._
 * import zio.schema.{ DeriveSchema, Schema }
 *
 * final case class Person(id: String, name: String)
 * object Person {
 *   implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
 *   val (id, name) = ProjectionExpression.accessors[Person]
 * }
 *
 * val save: DynamoDBQuery[Person, Option[Person]] = DynamoDBQuery.put("people", Person("1", "Ann"))
 * val load: DynamoDBQuery[Person, Either[DynamoDBError.ItemError, Person]] =
 *   DynamoDBQuery.get("people")(Person.id.partitionKey === "1")
 *
 * val program: ZIO[DynamoDBExecutor, DynamoDBError, Either[DynamoDBError.ItemError, Person]] =
 *   save.execute *> load.execute
 * // with DynamoDBExecutor.test("people" -> "id"), program succeeds with Right(Person(1,Ann))
 * }}}
 *
 * @see [[https://zio.dev/zio-dynamodb/reference/dynamodb-query/ DynamoDBQuery]]
 */
sealed trait DynamoDBQuery[-In, +Out] { self =>

  /**
   * Alias for `zipLeft`: runs both queries and keeps the result of this one.
   */
  final def <*[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, Out] = zipLeft(that)

  /**
   * Alias for `zipRight`: runs both queries and keeps the result of `that`.
   */
  final def *>[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, B] = zipRight(that)

  /**
   * Alias for `zip`: runs both queries and returns both results.
   */
  final def <*>[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, (Out, B)] = self zip that

  /**
   * Returns an effect that runs the query with the `DynamoDBExecutor` from the environment. A composite query is split
   * into its parts: when there are two or more gets, the gets that qualify are sent as one `BatchGetItem` request
   * (likewise for puts and deletes, as one `BatchWriteItem` request), and the other parts run in parallel. On the
   * query that `DynamoDBQuery.batch` returns, the effect fails with `DynamoDBError.BatchError.UnbatchableQueryError`,
   * and sends nothing, when more than one part would run unbatched; a get or a write that is the only one of its kind
   * counts as unbatched.
   *
   * Currently a query made with `DynamoDBQuery.fail`, or with `DynamoDBQuery.fromEither` from a `Left`, does not
   * fail. The effect succeeds with the error as its result (also inside `zip`, `zipLeft` and `zipRight`), or dies with
   * a `java.lang.ClassCastException` when the query is mapped with `map`. Also, a `DynamoDBError.ItemError.DecodingError` of the High Level `scanSome` or
   * `querySome` makes the effect die instead of failing.
   *
   * @see [[https://zio.dev/zio-dynamodb/reference/auto-batching-and-parallelisation/ Auto Batching]]
   */
  def execute: ZIO[DynamoDBExecutor, DynamoDBError, Out] = {
    val validateBatching = self match {
      case DynamoDBQuery.Map(Zip(_, _, _, validateBatching), _) =>
        validateBatching
      case _                                                    =>
        false
    }

    val (constructors: Chunk[DynamoDBQuery.Constructor[In, Any]], assembler: Function1[Chunk[Any], Out]) = parallelize(
      self
    )
    val (
      indexedConstructors: Chunk[(DynamoDBQuery.Constructor[In, Any], Int)],
      (batchGetItem: BatchGetItem, batchGetIndexes: Chunk[Int]),
      (batchWriteItem: BatchWriteItem, batchWriteIndexes: Chunk[Int]),
      decisions
    )                                                                                                    = batched(constructors)

    val indexedNonBatchedResults: ZIO[DynamoDBExecutor, DynamoDBError, Chunk[(Any, Int)]] =
      ZIO.foreachPar(indexedConstructors) { case (constructor, index) =>
        ddbExecute(constructor).map(result => (result, index))
      }

    val indexedGetResults: ZIO[DynamoDBExecutor, DynamoDBError, Chunk[(Option[AttrMap], Int)]] =
      ddbExecute(batchGetItem).flatMap {
        case resp @ BatchGetItem.Response(_, unprocessedKeys) if unprocessedKeys.size == 0 =>
          ZIO.succeed(batchGetItem.toGetItemResponses(resp) zip batchGetIndexes)
        case resp                                                                          =>
          ZIO.fail(resp.toErrorResponse)
      }

    val indexedWriteResults: ZIO[DynamoDBExecutor, DynamoDBError, Chunk[(Option[Any], Int)]] =
      ddbExecute(batchWriteItem).flatMap {
        case BatchWriteItem.Response(unprocessedItems) if unprocessedItems.size == 0 =>
          ZIO.succeed(batchWriteItem.addList.map(_ => None) zip batchWriteIndexes)
        case resp                                                                    =>
          ZIO.fail(resp.toErrorResponse)
      }

    val chunkOfResults: Chunk[ZIO[DynamoDBExecutor, DynamoDBError, Chunk[(Any, Int)]]] =
      Chunk.fromIterable(
        List(
          if (indexedConstructors.nonEmpty) Some(indexedNonBatchedResults) else None,
          if (batchGetIndexes.nonEmpty) Some(indexedGetResults) else None,
          if (batchWriteIndexes.nonEmpty) Some(indexedWriteResults) else None
        ).flatten
      )

    val result: ZIO[zio.dynamodb.DynamoDBExecutor, DynamoDBError, Out] = for {
      result <- ZIO.collectAllPar(chunkOfResults).map { xs =>
                  val combined: Chunk[(Any, Int)] = xs.flatten
                  val sortedValues: Chunk[Any]    = combined.sortBy { case (_, index) =>
                    index
                  }.map { case (value, _) => value }
                  val out: Out                    = assembler(sortedValues)
                  out
                }
    } yield result

    if (validateBatching && indexedConstructors.size > 1) // batch of one item results in a single non batched query
      ZIO.fail(
        DynamoDBError.BatchError.UnbatchableQueryError(decisions)
      )
    else
      result
  }

  /**
   * Reads from the secondary index `indexName`. Applies to scans and queries.
   */
  final def indexName(indexName: String): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.indexName(indexName), right.indexName(indexName), zippable)
      case Map(query, mapper)            => Map(query.indexName(indexName), mapper)
      case Absolve(query)                => Absolve(query.indexName(indexName))
      case q: ScanAll                    =>
        q.copy(indexName = Some(IndexName(indexName))).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: ScanSome                   =>
        q.copy(indexName = Some(IndexName(indexName))).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: QueryAll                   =>
        q.copy(indexName = Some(IndexName(indexName))).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: QuerySome                  =>
        q.copy(indexName = Some(IndexName(indexName))).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets `ReturnConsumedCapacity` (`Indexes`, `Total` or `None`, the default) on gets, puts, updates, deletes, scans,
   * queries and transactions. The result of the query does not include the consumed capacity. Currently a get, put
   * or delete that `execute` batches is sent without it.
   */
  final def capacity(capacity: ReturnConsumedCapacity): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.capacity(capacity), right.capacity(capacity), zippable)
      case Map(query, mapper)            => Map(query.capacity(capacity), mapper)
      case Absolve(query)                => Absolve(query.capacity(capacity))
      case g: GetItem                    =>
        g.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case b: BatchGetItem               =>
        b.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case b: BatchWriteItem             =>
        b.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: ScanAll                    =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: ScanSome                   =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: QueryAll                   =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: QuerySome                  =>
        q.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case m: PutItem                    =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case m: UpdateItem                 =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case m: DeleteItem                 =>
        m.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case t: Transaction[_]             =>
        t.copy(capacity = capacity).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets the read consistency, `ConsistencyMode.Strong` or `ConsistencyMode.Weak` (the default), on gets, scans and
   * queries. Currently, when `DynamoDBQuery#execute` sends several gets as one batch get request, no consistent read
   * flag is sent, so those reads are eventually consistent whatever the mode.
   */
  final def consistency(consistency: ConsistencyMode): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.consistency(consistency), right.consistency(consistency), zippable)
      case Map(query, mapper)            => Map(query.consistency(consistency), mapper)
      case Absolve(query)                => Absolve(query.consistency(consistency))
      case g: GetItem                    =>
        g.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: ScanAll                    =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: ScanSome                   =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: QueryAll                   =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case q: QuerySome                  =>
        q.copy(consistency = consistency).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets `ReturnValues`, the attributes that DynamoDB returns, on puts, updates and deletes. DynamoDB accepts only
   * `ReturnValues.None` (the default) and `ReturnValues.AllOld` on puts and deletes. A put or delete with another value
   * than `ReturnValues.None` is not batched.
   *
   * Note for `update(...)` ATM both ReturnValues.UpdatedNew and ReturnValues.UpdatedOld will potentially cause a decode
   * error for the high level API if all the attributes are not updated, as this will result in partial data being
   * returned; `update` then returns `None`, so they should not be used with it.
   *
   * If these are required then use the low level API (`updateItem`) for now.
   */
  def returns(returnValues: ReturnValues): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.returns(returnValues), right.returns(returnValues), zippable)
      case Map(query, mapper)            => Map(query.returns(returnValues), mapper)
      case Absolve(query)                => Absolve(query.returns(returnValues))
      case p: PutItem                    =>
        p.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: UpdateItem                 =>
        u.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DeleteItem                 =>
        d.copy(returnValues = returnValues).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets the condition of a put, update or delete, replacing an earlier one. When the condition is false, DynamoDB
   * rejects the write and the query fails with a `DynamoDBError.AWSError` holding a
   * `ConditionalCheckFailedException`. A put or delete with a condition is not batched.
   * {{{
   * import zio.dynamodb.ProjectionExpression.\$
   *
   * val insertOnly: DynamoDBQuery[Any, Option[Item]] =
   *   DynamoDBQuery.putItem("people", Item("id" -> "1", "name" -> "Ann")).where(\$("id").notExists)
   * }}}
   */
  def where[B](conditionExpression: ConditionExpression[B])(implicit ev: CanWhere[B, Out]): DynamoDBQuery[In, Out] = {
    val _ = ev
    self match {
      case zip @ Zip(left, right, zippable, _) =>
        Zip(
          left.where(conditionExpression.asInstanceOf[ConditionExpression[zip.Left]]),
          right.where(conditionExpression.asInstanceOf[ConditionExpression[zip.Right]]),
          zippable
        )
      case map @ Map(query, mapper)            =>
        Map(query.where(conditionExpression.asInstanceOf[ConditionExpression[map.Old]]), mapper)
      case ab @ Absolve(query)                 =>
        Absolve(query.where(conditionExpression.asInstanceOf[ConditionExpression[ab.Old]]))
      case p: PutItem                          =>
        p.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: UpdateItem                       =>
        u.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DeleteItem                       =>
        d.copy(conditionExpression = Some(conditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                                   => self
    }
  }

  /**
   * Sets `ReturnItemCollectionMetrics` (`Size` or `None`, the default) on puts, updates, deletes and transactions. The
   * result of the query does not include the metrics. Currently a put or delete that `execute` batches is sent
   * without it.
   */
  def metrics(itemMetrics: ReturnItemCollectionMetrics): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.metrics(itemMetrics), right.metrics(itemMetrics), zippable)
      case Map(query, mapper)            => Map(query.metrics(itemMetrics), mapper)
      case Absolve(query)                => Absolve(query.metrics(itemMetrics))
      case p: PutItem                    =>
        p.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case u: UpdateItem                 =>
        u.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DeleteItem                 =>
        d.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case t: Transaction[_]             =>
        t.copy(itemMetrics = itemMetrics).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets the exclusive start key of a scan or query: pass the `LastEvaluatedKey` that the previous page returned to
   * read the next page.
   */
  def startKey(exclusiveStartKey: LastEvaluatedKey): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) =>
        Zip(left.startKey(exclusiveStartKey), right.startKey(exclusiveStartKey), zippable)
      case Map(query, mapper)            => Map(query.startKey(exclusiveStartKey), mapper)
      case Absolve(query)                => Absolve(query.startKey(exclusiveStartKey))
      case s: ScanSome                   => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: ScanAll                    => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: QuerySome                  => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: QueryAll                   => s.copy(exclusiveStartKey = exclusiveStartKey).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Filter a Scan or a Query: sets the filter expression, replacing an earlier one. DynamoDB applies it after reading
   * the items, and returns only the items that match.
   */
  def filter[B](filterExpression: FilterExpression[B])(implicit ev: CanFilter[B, Out]): DynamoDBQuery[In, Out] = {
    val _ = ev
    self match {
      case zip @ Zip(left, right, zippable, _) =>
        Zip(
          left.filter(filterExpression.asInstanceOf[FilterExpression[zip.Left]]),
          right.filter(filterExpression.asInstanceOf[FilterExpression[zip.Right]]),
          zippable
        )
      case map @ Map(query, mapper)            =>
        Map(query.filter(filterExpression.asInstanceOf[FilterExpression[map.Old]]), mapper)
      case ab @ Absolve(query)                 =>
        Absolve(query.filter(filterExpression.asInstanceOf[FilterExpression[ab.Old]]))

      case s: ScanSome  => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: ScanAll   => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: QuerySome => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: QueryAll  => s.copy(filterExpression = Some(filterExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _            => self
    }
  }

  /**
   * Executes a DynamoDB Scan in parallel. Applies to `scanAll` and `scanAllItem`: when `n` is greater than 1, the
   * table is scanned as `n` segments at the same time, and their items are merged into one stream. There are no
   * guarantees on order of returned items.
   *
   * @param n The number of segments, each read by its own scan requests
   */
  def parallel(n: Int): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.parallel(n), right.parallel(n), zippable)
      case Map(query, mapper)            => Map(query.parallel(n), mapper)
      case Absolve(query)                => Absolve(query.parallel(n))
      case s: ScanAll                    => s.copy(totalSegments = n).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Adds a global secondary index with provisioned throughput (`readCapacityUnit` and `writeCapacityUnit`) to the
   * table that a `createTable` query creates.
   */
  def gsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType,
    readCapacityUnit: Long,
    writeCapacityUnit: Long
  ): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) =>
        Zip(
          left.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit),
          right.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit),
          zippable
        )
      case Map(query, mapper)            =>
        Map(query.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit), mapper)
      case Absolve(query)                =>
        Absolve(query.gsi(indexName, keySchema, projection, readCapacityUnit, writeCapacityUnit))
      case s: CreateTable                =>
        s.copy(globalSecondaryIndexes =
          s.globalSecondaryIndexes + GlobalSecondaryIndex(
            indexName,
            keySchema,
            projection,
            Some(ProvisionedThroughput(readCapacityUnit, writeCapacityUnit))
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Adds a global secondary index without provisioned throughput, as on a `BillingMode.PayPerRequest` table, to the
   * table that a `createTable` query creates.
   */
  def gsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType
  ): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) =>
        Zip(
          left.gsi(indexName, keySchema, projection),
          right.gsi(indexName, keySchema, projection),
          zippable
        )
      case Map(query, mapper)            => Map(query.gsi(indexName, keySchema, projection), mapper)
      case Absolve(query)                => Absolve(query.gsi(indexName, keySchema, projection))
      case s: CreateTable                =>
        s.copy(globalSecondaryIndexes =
          s.globalSecondaryIndexes + GlobalSecondaryIndex(
            indexName,
            keySchema,
            projection,
            None
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Adds a local secondary index to the table that a `createTable` query creates. `projection` defaults to
   * `ProjectionType.All`.
   */
  def lsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType = ProjectionType.All
  ): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) =>
        Zip(left.lsi(indexName, keySchema, projection), right.lsi(indexName, keySchema, projection), zippable)
      case Map(query, mapper)            => Map(query.lsi(indexName, keySchema, projection), mapper)
      case Absolve(query)                => Absolve(query.lsi(indexName, keySchema, projection))
      case s: CreateTable                =>
        s.copy(localSecondaryIndexes =
          s.localSecondaryIndexes + LocalSecondaryIndex(
            indexName,
            keySchema,
            projection
          )
        ).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets `Select` to `ALL_ATTRIBUTES` on a scan or query: DynamoDB returns every attribute of each item. The scan and
   * query constructors already set it when no projection is given.
   */
  def selectAllAttributes: DynamoDBQuery[In, Out] = select(Select.AllAttributes)

  /**
   * Sets `Select` to `ALL_PROJECTED_ATTRIBUTES` on a scan or query of an index: DynamoDB returns the attributes
   * projected into the index.
   */
  def selectAllProjectedAttributes: DynamoDBQuery[In, Out] = select(Select.AllProjectedAttributes)

  /**
   * Sets `Select` to `SPECIFIC_ATTRIBUTES` on a scan or query: DynamoDB returns the attributes of the projections.
   * The scan and query constructors already set it when projections are given.
   */
  def selectSpecificAttributes: DynamoDBQuery[In, Out] = select(Select.SpecificAttributes)

  /**
   * Sets `Select` to `COUNT` on a scan or query: DynamoDB returns the number of matching items instead of the items.
   * The result of the query does not include that number.
   */
  def selectCount: DynamoDBQuery[In, Out] = select(Select.Count)

  /**
   * Adds a KeyConditionExpr to a DynamoDBQuery: sets the key condition of a query (`queryAll`, `querySome` and their
   * Low Level forms), which DynamoDB requires. Example:
   * {{{
   * import zio.dynamodb.ProjectionExpression.\$
   * import zio.schema.{ DeriveSchema, Schema }
   *
   * final case class Student(email: String, subject: String)
   * object Student {
   *   implicit val schema: Schema.CaseClass2[String, String, Student] = DeriveSchema.gen[Student]
   *   val (email, subject) = ProjectionExpression.accessors[Student]
   * }
   *
   * // high level type safe API: "email" and "subject" are defined with ProjectionExpression.accessors[Student]
   * val highLevel = DynamoDBQuery
   *   .queryAll[Student]("student")
   *   .whereKey(Student.email.partitionKey === "avi@gmail.com" && Student.subject.sortKey === "maths")
   *
   * // low level API
   * val lowLevel = DynamoDBQuery
   *   .queryAllItem("student")
   *   .whereKey(\$("email").partitionKey === "avi@gmail.com" && \$("subject").sortKey === "maths")
   * }}}
   */
  def whereKey[From](keyConditionExpression: KeyConditionExpr[From]): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) =>
        Zip(left.whereKey(keyConditionExpression), right.whereKey(keyConditionExpression), zippable)
      case Map(query, mapper)            => Map(query.whereKey(keyConditionExpression), mapper)
      case Absolve(query)                => Absolve(query.whereKey(keyConditionExpression))

      case s: QuerySome =>
        s.copy(keyConditionExpr = Some(keyConditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: QueryAll  =>
        s.copy(keyConditionExpr = Some(keyConditionExpression)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _            => self
    }

  /**
   * Sets the policy that retries the unprocessed items of the batch request that this get, put or delete joins when it
   * is batched. A get, put or delete sent on its own does not use it. A batch request takes the first policy set by one
   * of its queries, or else `Schedule.recurs(3) && Schedule.exponential(50.milliseconds).jittered(0, 10)`.
   */
  def withRetryPolicy(retryPolicy: Schedule[Any, Throwable, Any]): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) =>
        Zip(left.withRetryPolicy(retryPolicy), right.withRetryPolicy(retryPolicy), zippable)
      case Map(query, mapper)            => Map(query.withRetryPolicy(retryPolicy), mapper)
      case Absolve(query)                => Absolve(query.withRetryPolicy(retryPolicy))
      case bw: BatchWriteItem            => bw.copy(retryPolicy = Some(retryPolicy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case bg: BatchGetItem              => bg.copy(retryPolicy = Some(retryPolicy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case p: PutItem                    => p.copy(retryPolicy = Some(retryPolicy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case d: DeleteItem                 => d.copy(retryPolicy = Some(retryPolicy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case g: GetItem                    => g.copy(retryPolicy = Some(retryPolicy)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets the sort key order of a query's results: ascending when `ascending` is `true` (the default), descending
   * otherwise.
   */
  def sortOrder(ascending: Boolean): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.sortOrder(ascending), right.sortOrder(ascending), zippable)
      case Map(query, mapper)            => Map(query.sortOrder(ascending), mapper)
      case Absolve(query)                => Absolve(query.sortOrder(ascending))
      case qs: QuerySome                 => qs.copy(ascending = ascending).asInstanceOf[DynamoDBQuery[In, Out]]
      case qa: QueryAll                  => qa.copy(ascending = ascending).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Sets the client request token of a query made with `transaction`; DynamoDB uses it to make a write transaction
   * idempotent. Call it after `transaction`: on other queries it has no effect.
   */
  def withClientRequestToken(token: String): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) =>
        Zip(left.withClientRequestToken(token), right.withClientRequestToken(token), zippable)
      case Map(query, mapper)            => Map(query.withClientRequestToken(token), mapper)
      case Absolve(query)                => Absolve(query.withClientRequestToken(token))
      case s @ Transaction(_, _, _, _)   => s.copy(clientRequestToken = Some(token))
      case _                             => self
    }

  /**
   * Returns a query that applies `f` to the result of this query.
   */
  final def map[B](f: Out => B): DynamoDBQuery[In, B] = DynamoDBQuery.Map(self, f)

  /**
   * Returns a query that runs this query and `that` together and returns both results, combined by `Zippable` into a
   * flat tuple. When executed, gets and writes of both that qualify are batched and the other parts run in parallel.
   * {{{
   * val both: DynamoDBQuery[Any, (Option[Item], Option[Item])] =
   *   DynamoDBQuery.getItem("people", PrimaryKey("id" -> "1")) zip
   *     DynamoDBQuery.getItem("people", PrimaryKey("id" -> "2"))
   * // sent as one BatchGetItem request; with only id 1 stored, the result is (Some(item1), None)
   * }}}
   */
  final def zip[In1 <: In, B](that: DynamoDBQuery[In1, B])(implicit
    z: Zippable[Out, B]
  ): DynamoDBQuery[In1, z.Out] =
    DynamoDBQuery.Zip[Out, B, z.Out](self, that, z, validateBatching = false)

  private[dynamodb] final def zip[In1 <: In, B](that: DynamoDBQuery[In1, B], validateBatching: Boolean)(implicit
    z: Zippable[Out, B]
  ): DynamoDBQuery[In1, z.Out] =
    DynamoDBQuery.Zip[Out, B, z.Out](self, that, z, validateBatching)

  /**
   * Like `zip`, but keeps only the result of this query.
   */
  final def zipLeft[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, Out] = (self zip that).map(_._1)

  /**
   * Like `zip`, but keeps only the result of `that`.
   */
  final def zipRight[In1 <: In, B](that: DynamoDBQuery[In1, B]): DynamoDBQuery[In1, B] = (self zip that).map(_._2)

  /**
   * Like `zip`, but combines the two results with `f`.
   */
  final def zipWith[In1 <: In, B, C](that: DynamoDBQuery[In1, B])(
    f: (Out, B) => C
  ): DynamoDBQuery[In1, C] =
    self.zip(that).map(f.tupled)

  private[dynamodb] final def zipWithValidateBatching[In1 <: In, B, C](that: DynamoDBQuery[In1, B])(
    f: (Out, B) => C
  ): DynamoDBQuery[In1, C] =
    self.zip(that, validateBatching = true).map(f.tupled)

  private def select(select: Select): DynamoDBQuery[In, Out] =
    self match {
      case Zip(left, right, zippable, _) => Zip(left.select(select), right.select(select), zippable)
      case Map(query, mapper)            => Map(query.select(select), mapper)
      case Absolve(query)                => Absolve(query.select(select))
      case s: ScanSome                   => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: ScanAll                    => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: QuerySome                  => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case s: QueryAll                   => s.copy(select = Some(select)).asInstanceOf[DynamoDBQuery[In, Out]]
      case _                             => self
    }

  /**
   * Returns a query that runs this query as one DynamoDB transaction: a `TransactGetItems` request when it holds only
   * gets, a `TransactWriteItems` request when it holds only puts, updates, deletes and condition checks. In a write
   * transaction, the results of the puts, updates and deletes are `None`. When the query cannot run as a transaction,
   * `execute` fails with a `DynamoDBError.TransactionError`; `safeTransaction` reports that before execution.
   * {{{
   * val transfer: DynamoDBQuery[Any, (Option[Item], Option[Item])] =
   *   (DynamoDBQuery.putItem("accounts", Item("id" -> "a", "balance" -> 90)) zip
   *     DynamoDBQuery.putItem("accounts", Item("id" -> "b", "balance" -> 110))).transaction
   * }}}
   *
   * @see [[https://zio.dev/zio-dynamodb/guides/transactions/ Transactions]]
   */
  final def transaction: DynamoDBQuery[In, Out] = Transaction(self).asInstanceOf[DynamoDBQuery[In, Out]]

  /**
   * Like `transaction`, but checks the query first: returns a `Left` with `DynamoDBError.TransactionError` when the
   * query mixes gets with writes, or holds a query that cannot run in a transaction (such as a scan).
   * {{{
   * val checked: Either[DynamoDBError.TransactionError, DynamoDBQuery[Any, (Option[Item], Option[Item])]] =
   *   (DynamoDBQuery.getItem("accounts", PrimaryKey("id" -> "a")) zip
   *     DynamoDBQuery.putItem("accounts", Item("id" -> "b"))).safeTransaction
   * // a Left holding DynamoDBError.TransactionError.MixedTransactionTypes
   * }}}
   */
  final def safeTransaction: Either[DynamoDBError.TransactionError, DynamoDBQuery[Any, Out]] = {
    val transaction = Transaction(self)
    DynamoDBExecutorImpl
      .buildTransaction(transaction)
      .flatMap { case (actions, _) =>
        DynamoDBExecutorImpl.filterMixedTransactions(actions)
      }
      .map(_ => transaction)
  }

}

/**
 * The constructors of `DynamoDBQuery`: High Level API queries that encode and decode a model type with its `Schema`,
 * Low Level API queries on `Item`s, table operations, `batch`, and pure values.
 */
object DynamoDBQuery {
  import scala.collection.immutable.{ Map => ScalaMap, Set => ScalaSet }

  /**
   * A query that is not built with `map` or `zip`: one DynamoDB operation, or, for `succeed` and `fail`, no request.
   * One operation can send several requests: a scan or query that reads all pages sends one request per page (and
   * one per segment after `parallel`), and a batch request resends unprocessed items. A `conditionCheck` sends
   * nothing outside a transaction.
   */
  sealed trait Constructor[-In, +A] extends DynamoDBQuery[In, A]

  /**
   * A query that can join a `BatchWriteItem` request: a put or a delete. `deleteItem` returns it.
   */
  sealed trait Write[-In, +A] extends Constructor[In, A]

  /**
   * A query that returns `a` without calling DynamoDB. `a` is evaluated each time the query runs. It also serves as the
   * starting value when folding a list of queries.
   */
  def succeed[A](a: => A): DynamoDBQuery[Any, A] = Succeed(() => a)

  /**
   * A query meant to fail with `e` without calling DynamoDB. Currently `DynamoDBQuery#execute` does not fail but
   * succeeds with `e` as its result (also inside `zip`, `zipLeft` and `zipRight`), and dies with a
   * `java.lang.ClassCastException` when the query is mapped with `map`.
   */
  def fail(e: => DynamoDBError): DynamoDBQuery[Any, Nothing] = Fail(() => e)

  private[dynamodb] def absolve[A, B](query: DynamoDBQuery[A, Either[ItemError, B]]): DynamoDBQuery[A, B] =
    Absolve(query)

  /**
   * Returns `succeed` of the value for a `Right`, and `fail` of the error for a `Left`, so a `Left` currently behaves
   * like `fail`: `DynamoDBQuery#execute` succeeds with the error as its result.
   */
  def fromEither[A](or: Either[ItemError, A]): DynamoDBQuery[Any, A] =
    or match {
      case Left(error)  => DynamoDBQuery.fail(error)
      case Right(value) => DynamoDBQuery.succeed(value)
    }

  /**
   * Deprecated: use `batch`, which returns the results as a `Chunk` instead of a `List`.
   *
   * Primary entry point for batching queries together. The function `body` is called for each `A` in the list and the
   * results are zipped together using a strict version of the zip operation. When the `execute` method is invoked on
   * the returned query, a `DynamoDBError.BatchError.UnbatchableQueryError` is returned with an explanation if more than
   * one of the queries would run unbatched; a get or a write that is the only one of its kind counts as unbatched.
   * Currently the check is lost once the returned query is changed, for example with `map` or an option setter such
   * as `capacity` or `withRetryPolicy`; the queries then run without it, the unbatchable ones in parallel. Currently
   * a batched get is also sent without its `consistency` and `capacity`, and a batched put or delete without its
   * `capacity` and `metrics`. For a full explanation of the batching rules please see
   * [[https://zio.dev/zio-dynamodb/reference/auto-batching-and-parallelisation/ Auto Batching and Parallelisation]]
   *
   * Note this is a low level function for a small amount of elements that is bounded by limits by the AWS API (at most
   * 25 writes or 100 gets per batch request) - if you want to perform a large number of reads and writes prefer the
   * following utility functions - `zio.dynamodb.batchReadItemFromStream`, `zio.dynamodb.batchReadFromStream`,
   * `zio.dynamodb.batchWriteFromStream` which work with ZStreams in a finite memory space and efficiently limit batch
   * sizes to the maximum size allowed by the AWS API, or alternatively use `batch` to implement your own streaming
   * functions.
   *
   * Note that if you need access to `unprocessedItems` or `unprocessedKeys` then an error handler for
   * `DynamoDBError.BatchError` should be provided.
   */
  @deprecated("Use batch instead", since = "")
  def forEach[In, A, B](values: Iterable[A])(body: A => DynamoDBQuery[In, B]): DynamoDBQuery[In, List[B]] =
    values.reverse.foldLeft[DynamoDBQuery[In, List[B]]](succeed(Nil)) {
      case (queryAcc: DynamoDBQuery[In, List[B]], a) =>
        val bodyApplied: DynamoDBQuery[In, B] = body(a)
        val zip: DynamoDBQuery[In, List[B]]   = bodyApplied.zipWithValidateBatching(queryAcc)(_ :: _)
        zip
    }

  /**
   * Primary entry point for batching queries together. The function `body` is called for each `A` in the list and the
   * results are zipped together using a strict version of the zip operation. When the `execute` method is invoked on
   * the returned query, a `DynamoDBError.BatchError.UnbatchableQueryError` is returned with an explanation if more than
   * one of the queries would run unbatched; a get or a write that is the only one of its kind counts as unbatched.
   * Currently the check is lost once the returned query is changed, for example with `map` or an option setter such
   * as `capacity` or `withRetryPolicy`; the queries then run without it, the unbatchable ones in parallel. Currently
   * a batched get is also sent without its `consistency` and `capacity`, and a batched put or delete without its
   * `capacity` and `metrics`. For a full explanation of the batching rules please see
   * [[https://zio.dev/zio-dynamodb/reference/auto-batching-and-parallelisation/ Auto Batching and Parallelisation]]
   *
   * Note this is a low level function for a small amount of elements that is bounded by limits by the AWS API (at most
   * 25 writes or 100 gets per batch request) - if you want to perform a large number of reads and writes prefer the
   * following utility functions - `zio.dynamodb.batchReadItemFromStream`, `zio.dynamodb.batchReadFromStream`,
   * `zio.dynamodb.batchWriteFromStream` which work with ZStreams in a finite memory space and efficiently limit batch
   * sizes to the maximum size allowed by the AWS API, or alternatively use `batch` to implement your own streaming
   * functions.
   *
   * Note that if you need access to `unprocessedItems` or `unprocessedKeys` then an error handler for
   * `DynamoDBError.BatchError` should be provided.
   *
   * The results come back in the order of `values`:
   * {{{
   * import zio._
   *
   * val people: DynamoDBQuery[Any, Chunk[Option[Item]]] =
   *   DynamoDBQuery.batch(List("3", "2", "1"))(id => DynamoDBQuery.getItem("people", PrimaryKey("id" -> id)))
   * // one BatchGetItem request; with ids 1 and 3 stored, the result is Chunk(Some(item3), None, Some(item1))
   * }}}
   */
  def batch[In, A, B](values: Iterable[A])(body: A => DynamoDBQuery[In, B]): DynamoDBQuery[In, Chunk[B]] =
    values.reverse.foldLeft[DynamoDBQuery[In, Chunk[B]]](succeed(Chunk.empty)) { case (queryAcc, a) =>
      val bodyApplied: DynamoDBQuery[In, B] = body(a)
      val zip: DynamoDBQuery[In, Chunk[B]]  = bodyApplied.zipWithValidateBatching(queryAcc)((el, acc) => el +: acc)
      zip
    }

  /**
   * Low Level API: reads the item with primary key `key` from the table `tableName`. The result is `None` when no item
   * has that key. With `projections`, DynamoDB returns only those attributes.
   */
  def getItem(
    tableName: String,
    key: PrimaryKey,
    projections: ProjectionExpression[_, _]*
  ): DynamoDBQuery[Any, Option[Item]] =
    GetItem(TableName(tableName), key, projections.toList)

  /**
   * High Level API: reads the item that `primaryKeyExpr` identifies from the table `tableName`, requesting the fields
   * of `From`, and decodes it. The result is a `Left` holding `DynamoDBError.ItemError.ValueNotFound` when no item has
   * that key, or `DynamoDBError.ItemError.DecodingError` when the item does not decode as `From`; `zio.dynamodb.syntax`
   * adds `maybeFound` to turn it into an `Option`.
   * {{{
   * import zio.schema.{ DeriveSchema, Schema }
   *
   * final case class Person(id: String, name: String)
   * object Person {
   *   implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
   *   val (id, name) = ProjectionExpression.accessors[Person]
   * }
   *
   * val ann: DynamoDBQuery[Person, Either[DynamoDBError.ItemError, Person]] =
   *   DynamoDBQuery.get("people")(Person.id.partitionKey === "1")
   * }}}
   */
  def get[From: Schema](tableName: String)(
    primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]
  ): DynamoDBQuery[From, Either[ItemError, From]] =
    get(tableName, primaryKeyExpr.asAttrMap, ProjectionExpression.projectionsFromSchema[From])

  /**
   * Sometimes we want to save top level sum types to DynamoDB and we want to retrieve them back as the subtype
   * with expressions in terms of the subtype as well.
   * `getWithNarrow` does a `get` with a safe narrow operation from type `From` to `To`.
   * If the narrow fails it returns a Decoding error with details of the cast failure in the message, such as
   * `failed to narrow - found type Unpaid but expected type Paid`.
   *
   * Requires implicit schemas in scope which ensure that `From` is an enum (sealed trait) and `To` is a subtype.
   *
   * Note this is an experimental API and may be subject to change.
   */
  def getWithNarrow[From: Schema.Enum, To <: From: Schema](tableName: String)(
    primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[To]
  ): DynamoDBQuery[From, Either[ItemError, To]] = {

    def getWithNarrowedKeyCondExpr[From: Schema.Enum, To <: From](tableName: String)(
      primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[To]
    ): DynamoDBQuery[From, Either[ItemError, From]] =
      get(tableName, primaryKeyExpr.asAttrMap, ProjectionExpression.projectionsFromSchema[From])

    getWithNarrowedKeyCondExpr[From, To](tableName)(primaryKeyExpr).map {
      case Right(found) =>
        narrow[From, To](found).left.map(DynamoDBError.ItemError.DecodingError.apply)

      case Left(error) => Left(error)
    }
  }

  /**
   * Safely narrows `a: From` to subtype type `To` and requires that there are implicit schemas in scope which
   * gives us a guarantee that `From` is an enum (sealed trait) and `To` is a subtype. Note that `To` itself can be an enum.
   */
  private[dynamodb] def narrow[From: Schema.Enum, To <: From: Schema](
    a: From
  ): Either[String, To] = {
    val fromEnumSchema: Schema.Enum[From]            = implicitly[Schema.Enum[From]]
    val toSchema: Schema[To]                         = Schema[To]
    val maybeSchemaOfA: Option[Schema.Case[From, _]] = fromEnumSchema.caseOf(a)

    val toSchemaId: String =
      toSchema match {
        case s: Schema.Enum[_]   => s.id.name
        case s: Schema.Record[_] => s.id.name
        case s: Schema[_]        => s.toString
      }

    def schemaOfAFoundInTargetSchema(schemaOfA: Schema[_], targetSchema: Schema[_]): Boolean =
      targetSchema match {
        case enumSchema: Schema.Enum[_] =>
          enumSchema.cases.exists { c =>
            c.schema match {
              case Schema.Lazy(s)    => s() == schemaOfA
              case e: Schema.Enum[_] => schemaOfAFoundInTargetSchema(schemaOfA, e)
              case _                 => false
            }
          }
        case _                          => false
      }

    maybeSchemaOfA match {
      case Some(c @ Schema.Case(_, Schema.Lazy(s), _, _, _, _)) =>
        val schemaOfA = s()
        if (schemaOfA == toSchema || schemaOfAFoundInTargetSchema(schemaOfA, toSchema))
          Right(a.asInstanceOf[To])
        else
          Left(
            s"failed to narrow - found type ${c.id} but expected type $toSchemaId"
          )
      case Some(c)                                              =>
        val schemaOfA = c.schema
        schemaOfA == toSchema || schemaOfAFoundInTargetSchema(schemaOfA, toSchema) match {
          case true => Right(a.asInstanceOf[To])
          case _    => Left(s"failed to narrow - found type ${c.id} but expected type $toSchemaId")
        }
      case None                                                 =>
        // this should never happen as we have a compile time proof
        Left(s"failed to narrow - argument is not a subtype of ${fromEnumSchema.id.name}")
    }
  }

  private def get[A: Schema](
    tableName: String,
    key: PrimaryKey,
    projections: Chunk[ProjectionExpression[_, _]]
  ): DynamoDBQuery[A, Either[ItemError, A]] =
    getItem(tableName, key, projections: _*).map {
      case Some(item) =>
        fromItem(item)
      case None       => Left(ValueNotFound(s"value with key $key not found"))
    }

  private[dynamodb] def fromItem[A: Schema](item: Item): Either[ItemError, A] = {
    val av = ToAttributeValue.attrMapToAttributeValue.toAttributeValue(item)
    av.decode(Schema[A])
  }

  /**
   * Low Level API: writes `item` to the table `tableName`, replacing any item with the same primary key.
   * `returns(ReturnValues.AllOld)` asks for the replaced item. Without it, the result is `None` when the put is batched
   * and with `DynamoDBExecutor.test`; a put sent on its own by `DynamoDBExecutor.live` currently returns `Some` of an
   * empty `Item`.
   */
  def putItem(tableName: String, item: Item): DynamoDBQuery[Any, Option[Item]] = PutItem(TableName(tableName), item)

  /**
   * High Level API: encodes `a` with its `Schema` and writes it to the table `tableName`, replacing any item with the
   * same primary key. The result is `None` unless `returns(ReturnValues.AllOld)` asks for the replaced item, and also
   * when that item does not decode as `A`.
   * {{{
   * import zio.schema.{ DeriveSchema, Schema }
   *
   * final case class Person(id: String, name: String)
   * object Person {
   *   implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
   * }
   *
   * val save: DynamoDBQuery[Person, Option[Person]] = DynamoDBQuery.put("people", Person("1", "Ann"))
   * }}}
   */
  def put[A: Schema](tableName: String, a: A): DynamoDBQuery[A, Option[A]] =
    putItem(tableName, toItem(a)).map(_.flatMap(item => fromItem(item).toOption))

  /**
   * It is common to save the top level sum type to DynamoDB and often we want to save them back as the subtype
   * with expressions in terms of the subtype as well.
   * `putWithNarrow` does a `put` of type `To` which is widened to type `From` before the save to ensure that the discriminator is saved,
   * and narrows the returned DynamoDBQuery to `To` .
   *
   * Requires implicit schemas in scope which ensure that `From` is an enum (sealed trait) and `To` is a subtype.
   *
   * Note this is an experimental API and may be subject to change.
   */
  def putWithNarrow[From: Schema.Enum, To <: From: Schema](
    tableName: String,
    a: To
  ): DynamoDBQuery[To, Option[To]] = {
    val fromEnumSchema = implicitly[Schema.Enum[From]]
    val toSchema       = Schema[To]
    putItem(tableName, toItem(a.asInstanceOf[From])(fromEnumSchema))
      .map(_.flatMap(item => fromItem(item)(toSchema).toOption))
  }

  private[dynamodb] def toItem[A](a: A)(implicit schema: Schema[A]): Item =
    FromAttributeValue.attrMapFromAttributeValue
      .fromAttributeValue(AttributeValue.encode(a)(schema))
      .getOrElse(throw new Exception(s"error encoding $a"))

  /**
   * High Level API: applies `action` to the item that `primaryKeyExpr` identifies in the table `tableName`; DynamoDB
   * creates the item when it does not exist. The result is `None` unless `returns` asks for attributes that decode as
   * `From`, such as `ReturnValues.AllNew`.
   * {{{
   * import zio.schema.{ DeriveSchema, Schema }
   *
   * final case class Person(id: String, name: String)
   * object Person {
   *   implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
   *   val (id, name) = ProjectionExpression.accessors[Person]
   * }
   *
   * val rename: DynamoDBQuery[Person, Option[Person]] =
   *   DynamoDBQuery.update("people")(Person.id.partitionKey === "1")(Person.name.set("Bob"))
   * }}}
   */
  def update[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From])(
    action: Action[From]
  ): DynamoDBQuery[From, Option[From]] =
    updateItem(tableName, primaryKeyExpr.asAttrMap)(action).map(_.flatMap(item => fromItem(item).toOption))

  /**
   * Low Level API: applies `action` to the item with primary key `key` in the table `tableName`; DynamoDB creates the
   * item when it does not exist. The result is `None` unless `returns` asks for attributes, which it then holds as
   * DynamoDB returned them.
   */
  def updateItem[A](tableName: String, key: PrimaryKey)(action: Action[A]): DynamoDBQuery[A, Option[Item]] =
    UpdateItem(
      TableName(tableName),
      key,
      UpdateExpression(action)
    )

  private[dynamodb] def update[A: Schema](tableName: String, key: PrimaryKey)(
    action: Action[A]
  ): DynamoDBQuery[A, Option[A]] =
    updateItem(tableName, key)(action).map(_.flatMap(item => fromItem(item).toOption))

  /**
   * Low Level API: deletes the item with primary key `key` from the table `tableName`.
   * `returns(ReturnValues.AllOld)` asks for the deleted item. Without it, the result is `None` when the delete is
   * batched and with `DynamoDBExecutor.test`; a delete sent on its own by `DynamoDBExecutor.live` currently returns
   * `Some` of an empty `Item`.
   */
  def deleteItem(tableName: String, key: PrimaryKey): Write[Any, Option[Item]] = DeleteItem(TableName(tableName), key)

  /**
   * High Level API: deletes the item that `primaryKeyExpr` identifies from the table `tableName`. The result is `None`
   * unless `returns(ReturnValues.AllOld)` asks for the deleted item, and also when that item does not decode as `From`.
   */
  def deleteFrom[From: Schema](
    tableName: String
  )(
    primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]
  ): DynamoDBQuery[Any, Option[From]] =
    deleteItem(tableName, primaryKeyExpr.asAttrMap).map(_.flatMap(item => fromItem(item).toOption))

  /**
   * Low Level API: scans the table `tableName` for one page of at most `limit` items. When executed will return a
   * Tuple of `(Chunk[Item], LastEvaluatedKey)`; pass the key to `startKey` to read the next page. With `projections`,
   * DynamoDB returns only those attributes.
   */
  def scanSomeItem(tableName: String, limit: Int, projections: ProjectionExpression[_, _]*): ScanSome =
    ScanSome(
      TableName(tableName),
      limit,
      select = selectOrAll(projections),
      projections = projections.toList
    )

  /**
   * High Level API: scans the table `tableName` for one page of at most `limit` items, requesting the fields of `A`.
   * When executed will return a Tuple of `(Chunk[A], LastEvaluatedKey)`; pass the key to `startKey` to read the next
   * page. Currently, when an item does not decode as `A`, the effect dies with the
   * `DynamoDBError.ItemError.DecodingError` instead of failing.
   */
  def scanSome[A: Schema](
    tableName: String,
    limit: Int
  ): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)] =
    DynamoDBQuery.absolve(
      scanSomeItem(tableName, limit, ProjectionExpression.projectionsFromSchema: _*).map { case (itemsChunk, lek) =>
        itemsChunk.forEach(item => fromItem(item)).map(Chunk.fromIterable) match {
          case Right(chunk) => Right((chunk, lek))
          case Left(error)  => Left(error)
        }
      }
    )

  /**
   * Low Level API: scans the whole table `tableName`. When executed will return a ZStream of Item, which reads the
   * pages as it is consumed. With `projections`, DynamoDB returns only those attributes.
   */
  def scanAllItem(tableName: String, projections: ProjectionExpression[_, _]*): ScanAll =
    ScanAll(
      TableName(tableName),
      select = selectOrAll(projections),
      projections = projections.toList
    )

  /**
   * High Level API: scans the whole table `tableName`, requesting the fields of `A`. When executed will return a
   * ZStream of A, which reads the pages as it is consumed; an item that does not decode as `A` fails the stream with a
   * `java.lang.IllegalStateException`.
   */
  def scanAll[A: Schema](
    tableName: String
  ): DynamoDBQuery[A, Stream[Throwable, A]] =
    scanAllItem(tableName, ProjectionExpression.projectionsFromSchema: _*).map(
      _.mapZIO(item => ZIO.fromEither(fromItem(item)).mapError(new IllegalStateException(_)))
    ) // TODO: think about error model

  /**
   * Low Level API: queries the table `tableName` for one page of at most `limit` items; set the key condition with
   * `whereKey`. When executed will return a Tuple of `(Chunk[Item], LastEvaluatedKey)`; pass the key to `startKey` to
   * read the next page. With `projections`, DynamoDB returns only those attributes.
   */
  def querySomeItem(tableName: String, limit: Int, projections: ProjectionExpression[_, _]*): QuerySome =
    QuerySome(
      TableName(tableName),
      limit,
      select = selectOrAll(projections),
      projections = projections.toList
    )

  /**
   * High Level API: queries the table `tableName` for one page of at most `limit` items, requesting the fields of
   * `A`; set the key condition with `whereKey`. When executed will return a Tuple of `(Chunk[A], LastEvaluatedKey)`;
   * pass the key to `startKey` to read the next page. Currently, when an item does not decode as `A`, the effect
   * dies with the `DynamoDBError.ItemError.DecodingError` instead of failing.
   */
  def querySome[A: Schema](
    tableName: String,
    limit: Int
  ): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)] =
    DynamoDBQuery.absolve(
      querySomeItem(tableName, limit, ProjectionExpression.projectionsFromSchema: _*).map { case (itemsChunk, lek) =>
        itemsChunk.forEach(item => fromItem(item)).map(Chunk.fromIterable) match {
          case Right(chunk) => Right((chunk, lek))
          case Left(error)  => Left(error)
        }
      }
    )

  /**
   * Low Level API: queries the table `tableName` for every matching item; set the key condition with `whereKey`. When
   * executed will return a ZStream of Item, which reads the pages as it is consumed. With `projections`, DynamoDB
   * returns only those attributes.
   */
  def queryAllItem(tableName: String, projections: ProjectionExpression[_, _]*): QueryAll =
    QueryAll(
      TableName(tableName),
      select = selectOrAll(projections),
      projections = projections.toList
    )

  /**
   * High Level API: queries the table `tableName` for every matching item, requesting the fields of `A`; set the key
   * condition with `whereKey`. When executed will return a ZStream of A, which reads the pages as it is consumed; an
   * item that does not decode as `A` fails the stream with a `java.lang.IllegalStateException`.
   */
  def queryAll[A: Schema](
    tableName: String
    //keyConditionExpression: KeyConditionExpression, REVIEW: This is required by the dynamo API, should we make it required here?
  ): DynamoDBQuery[A, Stream[Throwable, A]] =
    queryAllItem(tableName, ProjectionExpression.projectionsFromSchema: _*).map(
      _.mapZIO(item => ZIO.fromEither(fromItem(item)).mapError(new IllegalStateException(_)))
    ) // TODO: think about error model

  /**
   * Creates the table `tableName` with the primary key `keySchema`, the billing mode, at least one attribute
   * definition (one for each key attribute), and optional server-side encryption and tags. Add secondary indexes with
   * `gsi` and `lsi`.
   */
  def createTable(
    tableName: String,
    keySchema: KeySchema,
    billingMode: BillingMode,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty
  )(attributeDefinition: AttributeDefinition, attributeDefinitions: AttributeDefinition*): CreateTable =
    CreateTable(
      tableName = TableName(tableName),
      keySchema = keySchema,
      attributeDefinitions = NonEmptySet(attributeDefinition, attributeDefinitions: _*),
      billingMode = billingMode,
      sseSpecification = sseSpecification,
      tags = tags
    )

  /**
   * A check, for a write transaction, that `conditionExpression` holds for the item with primary key `primaryKey`: the
   * whole transaction fails when it does not. Zip it with the writes and call `transaction`; executed outside a
   * transaction, it sends nothing to DynamoDB.
   */
  def conditionCheck(
    tableName: String,
    primaryKey: PrimaryKey
  )(conditionExpression: ConditionExpression[_]): ConditionCheck =
    ConditionCheck(
      TableName(tableName),
      primaryKey,
      conditionExpression
    )

  /**
   * Deletes the table `tableName` and its items.
   */
  def deleteTable(
    tableName: String
  ): DeleteTable = DeleteTable(tableName = TableName(tableName))

  /**
   * Returns the ARN, status, size in bytes and item count of the table `tableName`.
   */
  def describeTable(
    tableName: String
  ): DescribeTable = DescribeTable(tableName = TableName(tableName))

  private def selectOrAll(projections: Seq[ProjectionExpression[_, _]]): Option[Select] =
    Some(if (projections.isEmpty) Select.AllAttributes else Select.SpecificAttributes)

  private[dynamodb] final case class Succeed[A](value: () => A) extends Constructor[Any, A]

  private[dynamodb] final case class Fail(error: () => DynamoDBError) extends Constructor[Any, Nothing]

  private[dynamodb] final case class GetItem(
    tableName: TableName,
    key: PrimaryKey,
    projections: List[ProjectionExpression[_, _]] =
      List.empty, // If no attribute names are specified, then all attributes are returned
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    retryPolicy: Option[Schedule[Any, Throwable, Any]] = None
  ) extends Constructor[Any, Option[Item]]

  private[dynamodb] final case class BatchRetryError() extends Throwable

  private[dynamodb] final case class BatchGetItem(
    requestItems: ScalaMap[TableName, BatchGetItem.TableGet] = ScalaMap.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    private[dynamodb] val orderedGetItems: Chunk[GetItem] =
      Chunk.empty, // track order of added GetItems for later unpacking
    retryPolicy: Option[Schedule[Any, Throwable, Any]] = None
  ) extends Constructor[Any, BatchGetItem.Response] { self =>

    def +(getItem: GetItem): BatchGetItem = {
      val tableName                                                     = getItem.tableName
      val key                                                           = getItem.key
      val projectionExpressionSet: ScalaSet[ProjectionExpression[_, _]] = getItem.projections.toSet
      val newEntry: (TableName, TableGet)                               =
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
      responses: MapOfSet[TableName, Item] = MapOfSet.empty,
      unprocessedKeys: ScalaMap[TableName, TableGet] = ScalaMap.empty
    ) { self =>
      def toErrorResponse: BatchError.GetError = {
        val unprocessedItems: ScalaMap[String, ScalaSet[PrimaryKey]] = self.unprocessedKeys.map {
          case (TableName(tableName), tableGet) => (tableName, tableGet.keysSet)
        }
        BatchError.GetError(unprocessedItems)
      }
    }
  }

  private[dynamodb] final case class Transaction[A](
    query: DynamoDBQuery[_, A],
    clientRequestToken: Option[String] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None
  ) extends Constructor[Any, A]

  private[dynamodb] final case class BatchWriteItem(
    requestItems: MapOfSet[TableName, BatchWriteItem.Write] = MapOfSet.empty,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    addList: Chunk[BatchWriteItem.Write] = Chunk.empty,
    retryPolicy: Option[Schedule[Any, Throwable, Any]] = None
  ) extends Constructor[Any, BatchWriteItem.Response] { self =>
    def +[A](writeItem: Write[Any, A]): BatchWriteItem =
      writeItem match {
        case putItem @ PutItem(_, _, _, _, _, _, _)       =>
          BatchWriteItem(
            self.requestItems + ((putItem.tableName, Put(putItem.item))),
            self.capacity,
            self.itemMetrics,
            self.addList :+ Put(putItem.item),
            self.retryPolicy.orElse(putItem.retryPolicy) // inherit retry policy from PutItem if not set
          )
        case deleteItem @ DeleteItem(_, _, _, _, _, _, _) =>
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
      unprocessedItems: Option[MapOfSet[TableName, BatchWriteItem.Write]]
    ) { self =>
      def toErrorResponse: BatchError.WriteError = {

        val unprocessedMap = self.unprocessedItems match {
          case Some(unprocessedItems) =>
            unprocessedItems.map { case (TableName(tableName), writesSet) =>
              (
                tableName,
                Chunk.fromIterable(writesSet.map {
                  case Delete(key) => BatchError.Delete(key)
                  case Put(item)   => BatchError.Put(item)
                })
              )
            }.toMap
          case None                   => ScalaMap.empty[String, Chunk[BatchError.Write]]
        }
        BatchError.WriteError(unprocessedMap)
      }
    }

  }
  private[dynamodb] final case class DeleteTable(
    tableName: TableName
  ) extends Constructor[Any, Unit]

  private[dynamodb] final case class DescribeTable(
    tableName: TableName
  ) extends Constructor[Any, DescribeTableResponse]

  /**
   * The status of a table, as `describeTable` reports it: one case for each value of DynamoDB's `TableStatus`.
   */
  sealed trait TableStatus

  /**
   * The cases of `TableStatus`.
   */
  object TableStatus {

    /**
     * The table is being created.
     */
    case object Creating extends TableStatus

    /**
     * The table or one of its indexes is being updated.
     */
    case object Updating extends TableStatus

    /**
     * The table is being deleted.
     */
    case object Deleting extends TableStatus

    /**
     * The table is ready for use.
     */
    case object Active extends TableStatus

    /**
     * DynamoDB cannot access the AWS KMS key that encrypts the table.
     */
    case object InaccessibleEncryptionCredentials extends TableStatus

    /**
     * The table is being archived.
     */
    case object Archiving extends TableStatus

    /**
     * The table has been archived.
     */
    case object Archived extends TableStatus

    /**
     * DynamoDB is not authorized to replicate the table.
     */
    case object ReplicationNotAuthorized extends TableStatus

    /**
     * A status that the version of the AWS SDK in use does not know.
     */
    case object unknownToSdkVersion extends TableStatus
  }

  // TODO: (adam) Add more fields here, this was for some basic testing initially
  /**
   * The result of `describeTable`: the table's ARN, its status, its size in bytes and its number of items.
   */
  final case class DescribeTableResponse(
    tableArn: String,
    tableStatus: TableStatus,
    tableSizeBytes: Long,
    itemCount: Long
  ) {
    override def toString: String =
      s"tableArn: $tableArn, tableStatus: $tableStatus, tableSizeBytes: $tableSizeBytes, itemCount: $itemCount"
  }

  // Interestingly scan can be run in parallel using segment number and total segments fields
  // If running in parallel segment number must be used consistently with the paging token
  // I have removed these fields on the assumption that the library will take care of these concerns
  private[dynamodb] final case class ScanSome(
    tableName: TableName,
    limit: Int,                                                 // TODO: should this be a long to match AWS API?
    indexName: Option[IndexName] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None                               // if ProjectExpression supplied then only valid value is SpecificAttributes
  ) extends Constructor[Any, (Chunk[Item], LastEvaluatedKey)]

  private[dynamodb] final case class QuerySome(
    tableName: TableName,
    limit: Int,                                                 // TODO: should this be a long to match AWS API?
    indexName: Option[IndexName] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    keyConditionExpr: Option[KeyConditionExpr[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                              // if ProjectExpression supplied then only valid value is SpecificAttributes
    ascending: Boolean = true
  ) extends Constructor[Any, (Chunk[Item], LastEvaluatedKey)]

  private[dynamodb] final case class ScanAll(
    tableName: TableName,
    indexName: Option[IndexName] = None,
    limit: Option[Int] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                              // if ProjectExpression supplied then only valid value is SpecificAttributes
    totalSegments: Int = 1
  ) extends Constructor[Any, Stream[Throwable, Item]]

  /**
   * Holds `ScanAll.Segment`, a segment of a parallel scan.
   */
  object ScanAll {

    /**
     * Segment `number` (counted from 0) of the `total` segments of a parallel scan (see `DynamoDBQuery#parallel`).
     */
    final case class Segment(number: Int, total: Int)
  }

  private[dynamodb] final case class QueryAll(
    tableName: TableName,
    indexName: Option[IndexName] = None,
    limit: Option[Int] = None,
    consistency: ConsistencyMode = ConsistencyMode.Weak,
    exclusiveStartKey: LastEvaluatedKey =
      None,                                                     // allows client to control start position - eg for client managed paging
    filterExpression: Option[FilterExpression[_]] = None,
    keyConditionExpr: Option[KeyConditionExpr[_]] = None,
    projections: List[ProjectionExpression[_, _]] = List.empty, // if empty all attributes will be returned
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    select: Option[Select] = None,                              // if ProjectExpression supplied then only valid value is SpecificAttributes
    ascending: Boolean = true
  ) extends Constructor[Any, Stream[Throwable, Item]]

  private[dynamodb] final case class PutItem(
    tableName: TableName,
    item: Item,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None, // PutItem does not recognize any values other than NONE or ALL_OLD.
    retryPolicy: Option[Schedule[Any, Throwable, Any]] = None
  ) extends Write[Any, Option[Item]]

  private[dynamodb] final case class UpdateItem(
    tableName: TableName,
    key: PrimaryKey,
    updateExpression: UpdateExpression[_],
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues = ReturnValues.None
  ) extends Constructor[Any, Option[Item]]

  private[dynamodb] final case class ConditionCheck(
    tableName: TableName,
    primaryKey: PrimaryKey,
    conditionExpression: ConditionExpression[_]
  ) extends Constructor[Any, Option[Item]]

  private[dynamodb] final case class DeleteItem(
    tableName: TableName,
    key: PrimaryKey,
    conditionExpression: Option[ConditionExpression[_]] = None,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None,
    itemMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.None,
    returnValues: ReturnValues =
      ReturnValues.None, // DeleteItem does not recognize any values other than NONE or ALL_OLD.
    retryPolicy: Option[Schedule[Any, Throwable, Any]] = None
  ) extends Write[Any, Option[Item]]

  private[dynamodb] final case class CreateTable(
    tableName: TableName,
    keySchema: KeySchema,
    attributeDefinitions: NonEmptySet[AttributeDefinition],
    billingMode: BillingMode,
    globalSecondaryIndexes: Set[GlobalSecondaryIndex] = Set.empty,
    localSecondaryIndexes: Set[LocalSecondaryIndex] = Set.empty,
    sseSpecification: Option[SSESpecification] = None,
    tags: ScalaMap[String, String] = ScalaMap.empty // you can have up to 50 tags
  ) extends Constructor[Any, Unit]

  private[dynamodb] final case class Zip[A, B, C](
    left: DynamoDBQuery[_, A],
    right: DynamoDBQuery[_, B],
    zippable: Zippable.Out[A, B, C],
    validateBatching: Boolean = false
  ) extends DynamoDBQuery[Any, C] {
    type Left  = A
    type Right = B
  }

  private[dynamodb] final case class Map[A, B](query: DynamoDBQuery[_, A], mapper: A => B)
      extends DynamoDBQuery[Any, B] {
    type Old = A
  }

  private[dynamodb] final case class Absolve[A, B](query: DynamoDBQuery[A, Either[ItemError, B]])
      extends DynamoDBQuery[A, B] {
    type Old = Either[ItemError, B]
  }

  /**
   * Same as `succeed`: a query that returns `a` without calling DynamoDB.
   */
  def apply[A](a: => A): DynamoDBQuery[Any, A] = Succeed(() => a)

  private[dynamodb] def batched[In](
    constructors: Chunk[Constructor[In, Any]]
  ): (Chunk[(Constructor[In, Any], Int)], (BatchGetItem, Chunk[Int]), (BatchWriteItem, Chunk[Int]), Set[String]) = {
    type IndexedConstructor = (Constructor[In, Any], Int)
    type IndexedGetItem     = (GetItem, Int)
    type IndexedWriteItem   = (Write[Any, Option[Any]], Int)

    def projectionsContainPrimaryKey(pes: List[ProjectionExpression[_, _]], pk: PrimaryKey): Boolean = {
      val matchedPrimaryKeys: List[Boolean] = pes.collect {
        case ProjectionExpression.MapElement(ProjectionExpression.Root, key) if pk.map.keySet.contains(key) => true
      }
      val hasNoProjections                  = pes.size == 0 // default is to return all attributes
      hasNoProjections || matchedPrimaryKeys.filter(_ == true).size == pk.map.size
    }

    val isSingleGetQuery   = constructors.count {
      case _: GetItem => true
      case _          => false
    } == 1 // single GetItem queries are not batched
    val isSingleWriteQuery = constructors.count {
      case _: Write[_, _] => true
      case _              => false
    } == 1 // single PutItem/DeleteItem queries are not batched

    val (indexedNonBatched, indexedGets, indexedWrites, decisions) =
      constructors.zipWithIndex
        .foldLeft[(Chunk[IndexedConstructor], Chunk[IndexedGetItem], Chunk[IndexedWriteItem], Set[String])](
          (Chunk.empty, Chunk.empty, Chunk.empty, Set.empty)
        ) {
          case ((nonBatched, gets, writes, decisions), (get @ GetItem(_, pk, pes, _, _, _), index)) =>
            if (isSingleGetQuery)
              (nonBatched :+ (get -> index), gets, writes, decisions + "single GetItem")
            else if (projectionsContainPrimaryKey(pes, pk))
              (
                nonBatched,
                gets :+ (get      -> index),
                writes,
                decisions + "multiple GetItem's"
              )
            else
              (nonBatched :+ (get -> index), gets, writes, decisions + "GetItem projections do not contain primary key")
          case (
                (nonBatched, gets, writes, decisions),
                (put @ PutItem(_, _, conditionExpression, _, _, returnValues, _), index)
              ) =>
            if (isSingleWriteQuery)
              (nonBatched :+ (put -> index), gets, writes, decisions + "single PutItem")
            else
              conditionExpression match {
                case Some(_) =>
                  (nonBatched :+ (put -> index), gets, writes, decisions + "PutItem has a condition expression")
                case None    =>
                  if (returnValues != ReturnValues.None)
                    (
                      nonBatched :+ (put -> index),
                      gets,
                      writes,
                      decisions + "PutItem has a return value other than None"
                    )
                  else
                    (
                      nonBatched,
                      gets,
                      writes :+ (put     -> index),
                      decisions + "multiple PutItem/DeleteItem"
                    )
              }
          case (
                (nonBatched, gets, writes, decisions),
                (delete @ DeleteItem(_, _, conditionExpression, _, _, returnValues, _), index)
              ) =>
            if (isSingleWriteQuery)
              (nonBatched :+ (delete -> index), gets, writes, decisions + "single DeleteItem")
            else
              conditionExpression match {
                case Some(_) =>
                  (nonBatched :+ (delete -> index), gets, writes, decisions + "DeleteItem has a condition expression")
                case None    =>
                  if (returnValues != ReturnValues.None)
                    (
                      nonBatched :+ (delete -> index),
                      gets,
                      writes,
                      decisions + "DeleteItem has a return value other than None"
                    )
                  else
                    (
                      nonBatched,
                      gets,
                      writes :+ (delete     -> index),
                      decisions + "multiple PutItem/DeleteItem"
                    )
              }
          case ((nonBatched, gets, writes, decisions), (nonBatchable, index))                       =>
            (
              nonBatched :+ (nonBatchable -> index),
              gets,
              writes,
              decisions + s"Query type ${nonBatchable.getClass.getSimpleName} not batchable"
            )
        }

    val indexedBatchGetItem: (BatchGetItem, Chunk[Int]) = indexedGets
      .foldLeft[(BatchGetItem, Chunk[Int])]((BatchGetItem(), Chunk.empty)) {
        case ((batchGetItem, indexes), (getItem, index)) => (batchGetItem + getItem, indexes :+ index)
      }

    val indexedBatchWrite: (BatchWriteItem, Chunk[Int]) = indexedWrites
      .foldLeft[(BatchWriteItem, Chunk[Int])]((BatchWriteItem(), Chunk.empty)) {
        case ((batchWriteItem, indexes), (writeItem, index)) => (batchWriteItem + writeItem, indexes :+ index)
      }

    (indexedNonBatched, indexedBatchGetItem, indexedBatchWrite, decisions)
  }

  @nowarn
  private[dynamodb] def parallelize[In, A](
    query: DynamoDBQuery[In, A]
  ): (Chunk[Constructor[In, Any]], Chunk[Any] => A) =
    query match {
      case Map(query, mapper) =>
        parallelize(query) match {
          case (constructors, assembler) =>
            (
              constructors.asInstanceOf[Chunk[Constructor[In, Any]]],
              assembler.andThen(mapper.asInstanceOf[(Any) => A])
            )
        }

      case zip @ Zip(_, _, _, _) =>
        val (constructorsLeft, assemblerLeft)   = parallelize(zip.left)
        val (constructorsRight, assemblerRight) = parallelize(zip.right)
        (
          (constructorsLeft ++ constructorsRight).asInstanceOf[Chunk[Constructor[In, Any]]],
          (results: Chunk[Any]) => {
            val (leftResults, rightResults) = results.splitAt(constructorsLeft.length)
            val left                        = assemblerLeft(leftResults)
            val right                       = assemblerRight(rightResults)
            zip.zippable.zip(left, right)
          }
        )

      case Absolve(query) =>
        val absolved: DynamoDBQuery[In, A] = query.map {
          case Left(dynamoDBError) => throw dynamoDBError
          case Right(a)            => a
        }
        parallelize(absolved)

      case Fail(error) =>
        (Chunk.empty, _ => error().asInstanceOf[A])

      case Succeed(value) => (Chunk.empty, _ => value())

      case batchGetItem @ BatchGetItem(_, _, _, _) =>
        (
          Chunk(batchGetItem),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case batchWriteItem @ BatchWriteItem(_, _, _, _, _) =>
        (
          Chunk(batchWriteItem),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case deleteTable @ DeleteTable(_) =>
        (
          Chunk(deleteTable),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case describeTable @ DescribeTable(_) =>
        (
          Chunk(describeTable),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      // condition check is not a real query, it is only used in transactions
      case _ @ConditionCheck(_, _, _)       =>
        (
          Chunk[Constructor[In, Any]](),
          (_: Chunk[Any]) => {
            ().asInstanceOf[A]
          }
        )

      case getItem @ GetItem(_, _, _, _, _, _) =>
        (
          Chunk(getItem),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case putItem @ PutItem(_, _, _, _, _, _, _) =>
        (
          Chunk(putItem),
          (results: Chunk[Any]) => {
            if (results.isEmpty) ().asInstanceOf[A] else results.head.asInstanceOf[A]
          }
        )

      case transaction @ Transaction(_, _, _, _) =>
        (
          Chunk(transaction),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case updateItem @ UpdateItem(_, _, _, _, _, _, _) =>
        (
          Chunk(updateItem),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case deleteItem @ DeleteItem(_, _, _, _, _, _, _) =>
        (
          Chunk(deleteItem),
          (results: Chunk[Any]) => {
            if (results.isEmpty) ().asInstanceOf[A] else results.head.asInstanceOf[A]
          }
        )

      case scan @ ScanSome(_, _, _, _, _, _, _, _, _) =>
        (
          Chunk(scan),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case scan @ ScanAll(_, _, _, _, _, _, _, _, _, _) =>
        (
          Chunk(scan),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case query @ QuerySome(_, _, _, _, _, _, _, _, _, _, _) =>
        (
          Chunk(query),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case query @ QueryAll(_, _, _, _, _, _, _, _, _, _, _) =>
        (
          Chunk(query),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

      case createTable @ CreateTable(_, _, _, _, _, _, _, _) =>
        (
          Chunk(createTable),
          (results: Chunk[Any]) => {
            results.head.asInstanceOf[A]
          }
        )

    }

}
