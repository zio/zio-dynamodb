package zio.dynamodb

import zio.dynamodb.TestDynamoDBExecutor.PkAndItem
import zio.{ UIO, ZIO }

/**
 * The control service of the in-memory executor that `DynamoDBExecutor.test` provides: it seeds tables and shows what
 * the code under test did. The executor is a Fake in memory implementation of `DynamoDBExecutor` that currently
 * provides bare minimum functionality to enable High and Low level API unit tests and examples that can serve as
 * documentation. Limited CRUD functionality is supported hence some features are currently not supported or have
 * restrictions.
 *  - Supported
 *    - CRUD operations GetItem, PutItem, DeleteItem, and gets, puts and deletes that are batched into BatchGetItem and
 *      BatchWriteItem requests
 *    - ScanSome, ScanAll, QuerySome, QueryAll: each returns the items of the table ordered by partition key, paged by
 *      the limit and the start key
 *  - Limited support
 *    - Primary Keys - each table is keyed by its partition key attribute only, so a get with a composite primary key
 *      (partition key and sort key) does not find an item stored by a put
 *  - Ignored
 *    - Projections - all fields are returned for all queries
 *    - Expressions - `KeyConditionExpression`'s, `ConditionExpression`'s and filters
 *    - Indexes and parallel scans in ScanSome, ScanAll, QuerySome, QueryAll
 *    - Return values - puts and deletes always return `None`
 *  - Not currently supported: the fiber dies with an exception whose message is `Constructor ... not implemented yet`
 *    - UpdateItem - this is a more complex case as it uses an expression to specify the update
 *    - Create table, Delete table, Describe table
 *    - Transactions
 *
 * '''Usage''': `DynamoDBExecutor.test` provides you the test DB instance in a `ZLayer`. Tables are created using the
 * `addTable` method in the test controller service `TestDynamoDBExecutor`. You specify a table, the name of its
 * partition key attribute and a var arg list of primary key/item pairs.
 * {{{
 * import zio._
 *
 * val ann: TestDynamoDBExecutor.PkAndItem = PrimaryKey("id" -> "1") -> Item("id" -> "1", "name" -> "Ann")
 *
 * val program: ZIO[DynamoDBExecutor with TestDynamoDBExecutor, DynamoDBError, Option[Item]] =
 *   for {
 *     _    <- TestDynamoDBExecutor.addTable("people", "id", ann)
 *     item <- DynamoDBQuery.getItem("people", PrimaryKey("id" -> "1")).execute
 *   } yield item
 *
 * val result: IO[DynamoDBError, Option[Item]] = program.provide(DynamoDBExecutor.test)
 * // succeeds with Some(AttrMap(Map(id -> String(1), name -> String(Ann))))
 * }}}
 *
 * @see [[https://zio.dev/zio-dynamodb/guides/testing/ Testing]]
 */
trait TestDynamoDBExecutor {

  /**
   * Creates the table `tableName`, whose partition key attribute is `pkFieldName`, holding `pkAndItems`. Each item is
   * stored under the primary key paired with it, which is not checked against the item. A table that already exists
   * is replaced.
   */
  def addTable(tableName: String, pkFieldName: String, pkAndItems: PkAndItem*): UIO[Unit]

  /**
   * Adds `pkAndItems` to the existing table `tableName`, replacing the items stored under the same primary keys.
   * Fails with `DynamoDBError.ItemError.ValueNotFound` when the table does not exist.
   */
  def addItems(tableName: String, pkAndItems: PkAndItem*): ZIO[Any, DynamoDBError, Unit]

  /**
   * Returns the queries the executor received, oldest first. `DynamoDBQuery#execute` sends a composite query in
   * parts, so it is recorded as its parts, with two or more gets or writes recorded as one batch request.
   */
  def recordedQueries: UIO[List[DynamoDBQuery[_, _]]]

  /**
   * Returns the items of the table `tableName`, or an empty set when the table does not exist.
   */
  def itemsForTable(tableName: String): UIO[Set[Item]]
}

/**
 * Accessors that call the `TestDynamoDBExecutor` service in the environment.
 */
object TestDynamoDBExecutor {

  /**
   * A primary key and the item stored under it.
   */
  type PkAndItem = (PrimaryKey, Item)

  /**
   * Calls `TestDynamoDBExecutor#addTable`: creates (or replaces) the table `tableName` whose partition key attribute
   * is `partitionKey`, holding `pkAndItems`.
   */
  def addTable(
    tableName: String,
    partitionKey: String,
    pkAndItems: PkAndItem*
  ): ZIO[TestDynamoDBExecutor, Nothing, Unit] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.addTable(tableName, partitionKey, pkAndItems: _*))

  /**
   * Calls `TestDynamoDBExecutor#addItems`: adds items to an existing table, failing with
   * `DynamoDBError.ItemError.ValueNotFound` when the table does not exist.
   */
  def addItems(tableName: String, pkAndItems: PkAndItem*): ZIO[TestDynamoDBExecutor, DynamoDBError, Unit] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.addItems(tableName, pkAndItems: _*))

  /**
   * Calls `TestDynamoDBExecutor#recordedQueries`: the queries the executor received, oldest first.
   */
  def recordedQueries: ZIO[TestDynamoDBExecutor, Nothing, List[DynamoDBQuery[_, _]]] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.recordedQueries)

  /**
   * Calls `TestDynamoDBExecutor#itemsForTable`: the items of a table, or an empty set when it does not exist.
   */
  def itemsForTable(tableName: String): ZIO[TestDynamoDBExecutor, Nothing, Set[Item]] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.itemsForTable(tableName))

  /**
   * Like `itemsForTable(tableName: String)`, for a `TableName`. `TableName` is internal to the library, so code
   * outside `zio.dynamodb` calls the `String` overload.
   */
  def itemsForTable(tableName: TableName): ZIO[TestDynamoDBExecutor, Nothing, Set[Item]] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.itemsForTable(tableName.value))
}
