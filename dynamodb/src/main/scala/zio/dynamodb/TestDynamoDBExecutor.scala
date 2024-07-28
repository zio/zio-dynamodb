package zio.dynamodb

import zio.{ UIO, ZIO }
import zio.dynamodb.DynamoDBQuery.{BatchGetItem, BatchWriteItem}

/**
 * A Fake implementation of `DynamoDBExecutor.Service` that currently has the very modest aspiration of providing bare minimum
 * functionality to enable internal unit tests and to enable simple end to end examples that can serve as documentation.
 * Limited CRUD functionality is supported hence some features are currently not supported or have restrictions.
 *  - Supported
 *    - CRUD operations GetItem, PutItem, DeleteItem, BatchGetItem, BatchWriteItem
 *  - Limited support
 *    - Primary Keys - only the partition key can be specified and is only checked for equality
 *  - Not currently supported
 *    - Projections - all fields are returned for all queries
 *    - Expressions - these include `KeyConditionExpression`'s, `ConditionExpression`'s, `ProjectionExpression`'s, `UpdateExpression`'s
 *    - Create table, Delete table
 *    - UpdateItem - this is a more complex case as it uses an expression to specify the update
 *    - Indexes in ScanSome, ScanAll, QuerySome, QueryAll
 *
 * '''Usage''': `DynamoDBExecutor.test` provides you the test DB instance in a `ZLayer`.
 * Tables are created using the `addTable` method in the test controller service `TestDynamoDBExecutor`. You specify
 * a table, a single primary and a var arg list of primary key/item pairs.
 * {{{
 * testM("getItem") {
 *   for {
 *     _ <- TestDynamoDBExecutor.addTable("tableName1", primaryKeyFieldName = "k1", primaryKey1 -> item1, primaryKey1_2 -> item1_2)
 *     result  <- GetItem(key = primaryKey1, tableName = tableName1).execute
 *     expected = Some(item1)
 *   } yield assert(result)(equalTo(expected))
 * }.provideLayer(DynamoDBExecutor.test)
 * }}}
 */
trait TestDynamoDBExecutor {
  def addTable(tableName: String, pkFieldName: String, pkAndItems: PkAndItem*): UIO[Unit]
  def addItems(tableName: String, pkAndItems: PkAndItem*): ZIO[Any, DynamoDBError, Unit]
  def queries: UIO[List[DynamoDBQuery[_, _]]]
}

object TestDynamoDBExecutor {

  def addTable(
    tableName: String,
    partitionKey: String,
    pkAndItems: PkAndItem*
  ): ZIO[TestDynamoDBExecutor, Nothing, Unit] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.addTable(tableName, partitionKey, pkAndItems: _*))

  def addItems(tableName: String, pkAndItems: PkAndItem*): ZIO[TestDynamoDBExecutor, DynamoDBError, Unit] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.addItems(tableName, pkAndItems: _*))

  def queries: ZIO[TestDynamoDBExecutor, Nothing, List[DynamoDBQuery[_, _]]] =
    ZIO.serviceWithZIO[TestDynamoDBExecutor](_.queries)

  def isEmptyBatchWrite(q: DynamoDBQuery[_, _]) =
    q match {
      case BatchWriteItem(items, _, _, _, _) => items.isEmpty
      case _                                 => false
    }

  def isEmptyBatchGet(q: DynamoDBQuery[_, _]) =
    q match {
      case BatchGetItem(items, _, _, _) => items.isEmpty
      case _                            => false
    }

}
