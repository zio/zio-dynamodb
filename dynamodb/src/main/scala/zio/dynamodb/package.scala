package zio

import zio.schema.Schema
import zio.stream.{ ZSink, ZStream }
import zio.dynamodb.proofs.IsPrimaryKey

package object dynamodb {
  // Filter expression is the same as a ConditionExpression but when used with Query but does not allow key attributes
  type FilterExpression[-From] = ConditionExpression[From]
  type LastEvaluatedKey        = Option[PrimaryKey]
  type PrimaryKey              = AttrMap
  val PrimaryKey = AttrMap
  type Item = AttrMap
  val Item = AttrMap

  type PkAndItem      = (PrimaryKey, Item)
  type TableNameAndPK = (String, String)

  type Encoder[A]  = A => AttributeValue
  type Decoder[+A] = AttributeValue => Either[DynamoDBError, A]

  private[dynamodb] def ddbExecute[A](query: DynamoDBQuery[_, A]): ZIO[DynamoDBExecutor, Throwable, A] =
    ZIO.serviceWithZIO[DynamoDBExecutor](_.execute(query))

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
  def batchWriteFromStream[R, A, In, B](
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(
    f: A => DynamoDBQuery[In, B]
  ): ZStream[DynamoDBExecutor with R, Throwable, B] = // TODO: Avi - can we constraint query to put or write?
    stream
      .aggregateAsync(ZSink.collectAllN[A](25))
      .mapZIOPar(mPar) { chunk =>
        val batchWriteItem = DynamoDBQuery
          .forEach(chunk)(a => f(a))
          .map(Chunk.fromIterable)
        for {
          r <- ZIO.environment[DynamoDBExecutor]
          b <- batchWriteItem.execute.provideEnvironment(r)
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
   * @return A stream of (A, Option[Item])
   */
  def batchReadItemFromStream[R, A](
    tableName: String,
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(
    pk: A => PrimaryKey
  ): ZStream[R with DynamoDBExecutor, Throwable, (A, Option[Item])] =
    stream
      .aggregateAsync(ZSink.collectAllN[A](100))
      .mapZIOPar(mPar) { chunk =>
        val batchGetItem: DynamoDBQuery[_, Chunk[(A, Option[Item])]] = DynamoDBQuery
          .forEach(chunk)(a =>
            DynamoDBQuery.getItem(tableName, pk(a)).map {
              case Some(item) => (a, Some(item))
              case None       => (a, None)
            }
          )
          .map(Chunk.fromIterable)
        for {
          r <- ZIO.environment[DynamoDBExecutor]
          list <- batchGetItem.execute.provideEnvironment(r)
        } yield list
      }
      .flattenChunks

  /**
   * Reads `stream` using function `pk` to determine the primary key which is then used to create a BatchGetItem request.
   * Stream is batched into groups of 100 items in a BatchGetItem and executed using the provided `DynamoDBExecutor` service
   * Returns a tuple of (A, Option[B]) where the option is None if the item is not found - this enables "LEFT outer
   * join" like functionality
   *
   * @param tableName
   * @param stream
   * @param mPar Level of parallelism for the stream processing
   * @param pk Function to determine the primary key
   * @tparam R Environment
   * @tparam A Input stream element type
   * @tparam B implicit Schema[B] where B is the type of the element in the returned stream
   * @return stream of Either[DynamoDBError.DecodingError, (A, Option[B])]
   */
  def batchReadFromStream2[R, A, From: Schema, To1: IsPrimaryKey, To2: IsPrimaryKey](
    tableName: String,
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(
    pk: A => PrimaryKeyExpr[From, To1, To2]
  ): ZStream[R with DynamoDBExecutor, Throwable, Either[DynamoDBError.DecodingError, (A, Option[From])]] =
    stream
      .aggregateAsync(ZSink.collectAllN[A](100))
      .mapZIOPar(mPar) { chunk =>
        val batchGetItem: DynamoDBQuery[From, Chunk[Either[DynamoDBError.DecodingError, (A, Option[From])]]] =
          DynamoDBQuery
            .forEach(chunk) { a =>
              DynamoDBQuery.get(tableName, pk(a)).map {
                case Right(b)                                 => Right((a, Some(b)))
                case Left(DynamoDBError.ValueNotFound(_))     => Right((a, None))
                case Left(e @ DynamoDBError.DecodingError(_)) => Left(e)
              }
            }
            .map(Chunk.fromIterable)
        for {
          r <- ZIO.environment[DynamoDBExecutor]
          list <- batchGetItem.execute.provideEnvironment(r)
        } yield list
      }
      .flattenChunks

}
