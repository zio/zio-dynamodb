package zio.dynamodb

import zio.aws.dynamodb.DynamoDb
import zio.stm.{ STM, TMap }
import zio.{ Ref, ULayer, URLayer, ZIO, ZLayer }

/**
 * The ZIO service that runs `DynamoDBQuery` values: `DynamoDBQuery#execute` returns an effect that needs it in the
 * environment. `DynamoDBExecutor.live` sends the requests to DynamoDB through a ZIO AWS `DynamoDb` client, and
 * `DynamoDBExecutor.test` keeps tables in memory for unit tests.
 *
 * The live layer needs a `DynamoDb` client, which needs an `AwsConfig` and an HTTP client. They take the region and
 * credentials from the AWS SDK's default provider chains:
 * {{{
 * import zio._
 * import zio.aws.core.config.AwsConfig
 * import zio.aws.dynamodb.DynamoDb
 * import zio.aws.netty.NettyHttpClient
 *
 * val executor: ZLayer[Any, Throwable, DynamoDBExecutor] =
 *   (NettyHttpClient.default >>> AwsConfig.default >>> DynamoDb.live) >>> DynamoDBExecutor.live
 *
 * val program: ZIO[Any, Throwable, Option[Item]] =
 *   DynamoDBQuery.getItem("people", PrimaryKey("id" -> "1")).execute.provideLayer(executor)
 * }}}
 *
 * @see [[https://zio.dev/zio-dynamodb/guides/getting-started/ Getting Started]]
 */
trait DynamoDBExecutor {

  /**
   * Runs one query and returns its result. Prefer `DynamoDBQuery#execute`, which batches and parallelises the parts
   * of a composite query before it calls this method for each part. Called directly with a zipped query, the live
   * executor runs the parts one after the other without batching, and the test executor supports only single
   * requests: other queries make the fiber die.
   */
  def execute[A](query: DynamoDBQuery[_, A]): ZIO[Any, DynamoDBError, A]
}

/**
 * The layers that provide a `DynamoDBExecutor`.
 */
object DynamoDBExecutor {

  /**
   * A layer that runs queries against DynamoDB with the ZIO AWS `DynamoDb` client from the environment. An AWS SDK
   * `DynamoDbException` fails the query with `DynamoDBError.AWSError`, and another non-fatal exception with
   * `DynamoDBError.UnknownNonFatalError`. Batch requests resend unprocessed items, by default
   * `Schedule.recurs(3) && Schedule.exponential(50.milliseconds).jittered(0, 10)`; `DynamoDBQuery#withRetryPolicy`
   * replaces that policy.
   */
  val live: URLayer[DynamoDb, DynamoDBExecutor] =
    ZLayer.fromZIO(for {
      db <- ZIO.service[DynamoDb]
    } yield DynamoDBExecutorImpl(db))

  /**
   * An in-memory executor for unit tests, which also provides the `TestDynamoDBExecutor` service. It starts with no
   * tables: create them with `TestDynamoDBExecutor.addTable`. Each time the layer is built, for example by each
   * `provide`, it starts again with no tables. See `TestDynamoDBExecutor` for the queries it supports.
   */
  lazy val test: ULayer[DynamoDBExecutor with TestDynamoDBExecutor] = {
    val effect = for {
      ref  <- Ref.make(List.empty[DynamoDBQuery[_, _]])
      test <- (for {
                tableMap       <- TMap.empty[TableName, TMap[PrimaryKey, Item]]
                tablePkNameMap <- TMap.empty[TableName, String]
              } yield TestDynamoDBExecutorImpl(ref, tableMap, tablePkNameMap)).commit
    } yield test
    ZLayer.fromZIO(effect)
  }

  /**
   * Like `DynamoDBExecutor.test` without arguments, but starts with one empty table for each pair of a table name and
   * the name of its partition key attribute, for example `DynamoDBExecutor.test("people" -> "id")`.
   */
  def test(tableAndPKNames: (String, String)*): ULayer[DynamoDBExecutor with TestDynamoDBExecutor] = {
    val effect = for {
      ref  <- Ref.make(List.empty[DynamoDBQuery[_, _]])
      test <- (for {
                tableMap       <- TMap.empty[TableName, TMap[PrimaryKey, Item]]
                tablePkNameMap <- TMap.empty[TableName, String]
                _              <- STM.foreach(tableAndPKNames) { case (tableName, pkFieldName) =>
                                    for {
                                      _           <- tablePkNameMap.put(TableName(tableName), pkFieldName)
                                      pkToItemMap <- TMap.empty[PrimaryKey, Item]
                                      _           <- tableMap.put(TableName(tableName), pkToItemMap)
                                    } yield ()
                                  }
              } yield TestDynamoDBExecutorImpl(ref, tableMap, tablePkNameMap)).commit
    } yield test
    ZLayer.fromZIO(effect)
  }

}
