package zio

import zio.clock.Clock
import zio.stream.{ Transducer, ZStream }

package object dynamodb {
  // Filter expression is the same as a ConditionExpression but when used with Query but does not allow key attributes
  type FilterExpression = ConditionExpression
  type LastEvaluatedKey = Option[PrimaryKey]
  type PrimaryKey       = AttrMap
  val PrimaryKey = AttrMap
  type Item = AttrMap
  val Item = AttrMap

  type TableEntry = (PrimaryKey, Item)

  type Encoder[A]  = A => AttributeValue
  type Decoder[+A] = AttributeValue => Either[String, A]

  private[dynamodb] def ddbExecute[A](query: DynamoDBQuery[A]): ZIO[Has[DynamoDBExecutor], Throwable, A] =
    ZIO.serviceWith[DynamoDBExecutor](_.execute(query))

  /**
   * Reads `stream` and uses function `f` for creating a BatchWrite request that is executes for side effects. Stream is batched into groups
   * of 25 items in a BatchWriteItem and executed using the `DynamoDBExecutor` service provided in the environment.
   * @param stream
   * @param mPar Level of parallelism for the stream processing
   * @param f Function that takes an `A` and returns a PutItem or WriteItem
   * @tparam R Environment
   * @tparam A
   * @tparam B Type of DynamoDBQuery returned by `f`
   * @return A stream of results from the `DynamoDBQuery` write's
   */
  def batchWriteFromStream[R, A, B](
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(f: A => DynamoDBQuery[B]): ZStream[Has[DynamoDBExecutor] with R with Clock, Throwable, B] =
    stream
      .aggregateAsync(Transducer.collectAllN(25))
      .mapMPar(mPar) { chunk =>
        val batchWriteItem = DynamoDBQuery
          .forEach(chunk)(a => f(a))
          .map(Chunk.fromIterable)
        for {
          r <- ZIO.environment[Has[DynamoDBExecutor]]
          b <- batchWriteItem.execute.provide(r)
        } yield b
      }
      .flattenChunks

  /**
   * Reads `stream` using function `pk` to determine the primary key which is then used to create a BatchGetItem request.
   * Stream is batched into groups of 100 items in a BatchGetItem and executed using the provided `DynamoDBExecutor` service
   * @param tableName
   * @param stream
   * @param mPar Level of parallelism for the stream processing
   * @param pk Function to determine the primary key
   * @tparam R Environment
   * @tparam A
   * @return A stream of Item
   */
  def batchReadFromStream[R, A](
    tableName: String,
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(
    pk: A => PrimaryKey
  ): ZStream[R with Has[DynamoDBExecutor] with Clock, Throwable, Item] =
    stream
      .aggregateAsync(Transducer.collectAllN(100))
      .mapMPar(mPar) { chunk =>
        val batchGetItem: DynamoDBQuery[Chunk[Option[Item]]] = DynamoDBQuery
          .forEach(chunk)(a => DynamoDBQuery.getItem(tableName, pk(a)))
          .map(Chunk.fromIterable)
        for {
          r    <- ZIO.environment[Has[DynamoDBExecutor]]
          list <- batchGetItem.execute.provide(r)
        } yield list
      }
      .flattenChunks
      .collectSome

}
