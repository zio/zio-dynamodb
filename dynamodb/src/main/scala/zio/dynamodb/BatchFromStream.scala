package zio.dynamodb

import zio.ZIO
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.stream.ZStream

object BatchFromStream {

  /**
   * Processes `stream` with side effecting function `f`. Stream is batched into groups of 25 items in a BatchWriteItem
   * and executed using the provided `DynamoDBExecutor` service
   * @param stream
   * @param mPar Level of parllelism for the stream processing
   * @param f Function that takes an `A` and returns a `DynamoDBQuery.Write` which are used internally to populate a BatchWriteItem request
   * @tparam R
   * @tparam A
   * @return
   */
  // capture unit as B type parameter
  def batchWriteFromStream[R, A](
    stream: ZStream[R, Exception, A],
    mPar: Int = 10
  )(f: A => DynamoDBQuery.Write[Unit]): ZIO[DynamoDBExecutor with R, Exception, Unit] =
    stream
      .grouped(25)
      .mapMPar(mPar) { chunk =>
        val zio = DynamoDBQuery
          .forEach(chunk)(a => f(a))
        for {
          r <- ZIO.environment[DynamoDBExecutor]
          _ <- zio.execute.provide(r)
        } yield ()
      }
      .runDrain

  /**
   * Processes `stream` with `BatchGetItem` requests using function `pk` to determine the primary key, and applies function
   * `f` to the returned
   * Stream is batched into groups of 100 items in a BatchWriteItem and executed using the provided `DynamoDBExecutor` service
   * @param tableName
   * @param stream
   * @param mPar Level of parllelism for the stream processing
   * @param pk
   * @param f
   * @tparam R
   * @tparam A
   * @return
   */
  def batchReadFromStream[R, A](
    tableName: String,
    stream: ZStream[R, Exception, A],
    mPar: Int = 10
  )(
    pk: A => PrimaryKey
  )(
    f: List[Option[Item]] => ZIO[R, Exception, Unit]
  ): ZIO[R with DynamoDBExecutor, Exception, Unit] = // TODO: think about returning a Stream
    stream
      .grouped(100)
      .mapMPar(mPar) { chunk =>
        val zio: DynamoDBQuery[List[Option[Item]]] = DynamoDBQuery
          .forEach(chunk)(a => DynamoDBQuery.getItem(tableName, pk(a)))
        for {
          r    <- ZIO.environment[DynamoDBExecutor]
          list <- zio.execute.provide(r)
          _    <- f(list)
        } yield ()
      }
      .runDrain

}
