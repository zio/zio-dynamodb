package zio

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.stream.ZStream

package object dynamodb {
  // Filter expression is the same as a ConditionExpression but when used with Query but does not allow key attributes
  type FilterExpression = ConditionExpression
  type LastEvaluatedKey = Option[PrimaryKey]
  type PrimaryKey       = AttrMap
  val PrimaryKey = AttrMap
  type Item = AttrMap
  val Item = AttrMap

  private[dynamodb] def ddbExecute[A](query: DynamoDBQuery[A]): ZIO[DynamoDBExecutor, Exception, A] =
    ZIO.accessM[DynamoDBExecutor](_.get.execute(query))

  /**
   * Processes `stream` with side effecting function `f`. Stream is batched into groups of 25 items in a BatchWriteItem
   * and executed using the `DynamoDBExecutor` service provided in the environment.
   * @param stream
   * @param mPar Level of parllelism for the stream processing
   * @param f Function that takes an `A` and returns a `DynamoDBQuery.Write` which are used internally to populate a BatchWriteItem request
   * @tparam R Environment
   * @tparam A
   * @tparam B Type of DynamoDBQuery.Write
   * @return A stream of results from the `DynamoDBQuery.Write`'s
   */
  def batchWriteFromStream[R, A, B](
    stream: ZStream[R, Exception, A],
    mPar: Int = 10
  )(f: A => DynamoDBQuery.Write[B]): ZStream[DynamoDBExecutor with R, Exception, B] =
    stream
      .grouped(25)
      .mapMPar(mPar) { chunk =>
        val zio = DynamoDBQuery
          .forEach(chunk)(a => f(a))
          .map(Chunk.fromIterable)
        for {
          r <- ZIO.environment[DynamoDBExecutor]
          b <- zio.execute.provide(r)
        } yield b
      }
      .flattenChunks

  /**
   * Processes `stream` with `BatchGetItem` requests using function `pk` to determine the primary key.
   * Stream is batched into groups of 100 items in a BatchWriteItem and executed using the provided `DynamoDBExecutor` service
   * @param tableName
   * @param stream
   * @param mPar Level of parllelism for the stream processing
   * @param pk Function to determine the primary key
   * @tparam R Environment
   * @tparam A
   * @return A stream of Item
   */
  def batchReadFromStream[R, A](
    tableName: String,
    stream: ZStream[R, Exception, A],
    mPar: Int = 10
  )(
    pk: A => PrimaryKey
  ): ZStream[R with DynamoDBExecutor, Exception, Item] =
    stream
      .grouped(100)
      .mapMPar(mPar) { chunk =>
        val zio: DynamoDBQuery[Chunk[Option[Item]]] = DynamoDBQuery
          .forEach(chunk)(a => DynamoDBQuery.getItem(tableName, pk(a)))
          .map(Chunk.fromIterable)
        for {
          r    <- ZIO.environment[DynamoDBExecutor]
          list <- zio.execute.provide(r)
        } yield list
      }
      .flattenChunks
      .collectSome

}
