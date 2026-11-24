package zio.dynamodb.interop.ce

import cats.arrow.FunctionK
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.std.Dispatcher
import cats.syntax.all._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import zio.{ Scope, Unsafe, ZIO, ZLayer }
import zio.aws.core.config
import zio.aws.dynamodb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb._
import zio.schema.Schema
import zio.stream.ZStream
import zio.stream.interop.fs2z._

import scala.concurrent.Future

/**
 * Cats Effect and FS2 interop for running a `DynamoDBQuery` as an `F[_]` effect: the `DynamoDBExceutorF` executor, the
 * `executeToF` method on queries, and FS2 versions of the streaming batch helpers.
 */
object syntax {

  /**
   * Converts the result type of a ZIO DynamoDB query to its Cats Effect counterpart. There is an instance for
   * `ZStream[Any, Throwable, A]`, which becomes an `fs2.Stream[F, A]`, and an instance for any other type, which stays
   * as it is.
   */
  trait CatsCompatible[ZioType] {

    /** The Cats Effect type that `ZioType` is converted to. */
    type Out

    /** Converts a ZIO result to its Cats Effect counterpart. */
    def toCats(a: ZioType): Out
  }

  /** A natural transformation from `F` to `zio.Task` that runs the `F` effect on the given `Dispatcher`. */
  def toZioFunctionK[F[_]](implicit d: Dispatcher[F]): FunctionK[F, zio.Task] =
    new FunctionK[F, zio.Task] {
      def apply[A](t: F[A]): zio.Task[A] = toZioEffect(t)
    }

  /** Runs `t` on the given `Dispatcher` and returns a `zio.Task` that completes with its result. */
  def toZioEffect[F[_], A](t: F[A])(implicit d: Dispatcher[F]): zio.Task[A] =
    ZIO.uninterruptibleMask { restore =>
      val future = d.unsafeToFuture(t)
      val zio    = ZIO.fromFuture(_ => future)
      restore(zio)
    }

  /** Instances of [[CatsCompatible]]. */
  object CatsCompatible extends CatsCompatibleLowPriority {

    /** A [[CatsCompatible]] for `A` whose `Out` is `Out0`. */
    type Aux[A, Out0] = CatsCompatible[A] { type Out = Out0 }

    /** A natural transformation from `zio.Task` to `F` that runs the task on `zio.Runtime.default`. */
    final def toCeFunctionK[F[_]](implicit F: Async[F]): FunctionK[zio.Task, F] =
      new FunctionK[zio.Task, F] {
        def apply[A](t: zio.Task[A]): F[A] = toEffect[F, A](t)
      }

    private def toEffect[F[_], A](t: zio.Task[A])(implicit F: Async[F]): F[A] =
      F.uncancelable { poll =>
        F.delay(
          Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.runToFuture(t))
        ).flatMap { future =>
          poll(F.onCancel(F.fromFuture(F.pure[Future[A]](future)), F.fromFuture(F.delay(future.cancel())).void))
        }
      }

    /** Converts a `ZStream[Any, Throwable, A]` to an `fs2.Stream[F, A]`. */
    implicit def zioStreamCatsCompatible[F[_], A](implicit
      F: Async[F]
    ): CatsCompatible.Aux[ZStream[Any, Throwable, A], fs2.Stream[F, A]] =
      new CatsCompatible[ZStream[Any, Throwable, A]] {
        type Out = fs2.Stream[F, A]

        def toCats(a: ZStream[Any, Throwable, A]): Out = a.toFs2Stream.translate(toCeFunctionK)
      }
  }

  /** Lower-priority instances of [[CatsCompatible]]. */
  trait CatsCompatibleLowPriority {

    /** Leaves a result of type `A` as it is. */
    implicit def catsCompatible[A]: CatsCompatible.Aux[A, A] =
      new CatsCompatible[A] {
        type Out = A

        def toCats(a: A): Out = a
      }
  }

  /**
   * Runs a `DynamoDBQuery` as an `F` effect on `zio.Runtime.default`, using the wrapped `DynamoDBExecutor`. Create it
   * with `DynamoDBExceutorF.of` or `DynamoDBExceutorF.ofCustomised`. The class name is spelt this way in the library.
   */
  class DynamoDBExceutorF[F[_]](val dynamoDBExecutor: DynamoDBExecutor)(implicit F: Async[F]) {

    /**
     * Runs `query` and returns its result as an `F` effect, converted by `ce`: a query that returns a `ZStream` gives
     * an `fs2.Stream`. A failed query fails the `F` with the `DynamoDBError`. Currently cancelling the returned `F`
     * does not interrupt the running query: the cancellation waits until the query has finished.
     */
    def execute[Out](query: DynamoDBQuery[_, Out])(implicit ce: CatsCompatible[Out]): F[ce.Out] =
      F.uncancelable { poll =>
        F.delay(
          Unsafe.unsafe(implicit u =>
            zio.Runtime.default.unsafe.runToFuture(dynamoDBExecutor.execute(query).map(ce.toCats(_)))
          )
        ).flatMap { future =>
          poll(F.onCancel(F.fromFuture(F.pure[Future[ce.Out]](future)), F.fromFuture(F.delay(future.cancel())).void))
        }
      }
  }

  /** Constructors of [[DynamoDBExceutorF]]. */
  object DynamoDBExceutorF {

    /**
     * A `Resource` that builds a `DynamoDBExceutorF` with the default AWS SDK client settings, and closes the client
     * when the resource is released.
     */
    def of[F[_]: Async]: Resource[F, DynamoDBExceutorF[F]] = ofCustomised(identity)

    /**
     * Like `of`, but passes the AWS SDK `DynamoDbAsyncClientBuilder` through `customization` before the client is
     * built, for example to set an endpoint override.
     */
    def ofCustomised[F[_]: Async](
      customization: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder
    ): Resource[F, DynamoDBExceutorF[F]] = {
      import zio.interop.catz._

      implicit val runtime = zio.Runtime.default

      val ddbLayer =
        NettyHttpClient.default >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized(customization)

      val scopedF: ZIO[Any with Scope, Throwable, DynamoDBExceutorF[F]] = for {
        zenv <- (ddbLayer >>> DynamoDBExecutor.live).build
      } yield new DynamoDBExceutorF[F](zenv.get[DynamoDBExecutor])
      val resource: Resource[F, DynamoDBExceutorF[F]]                   = Resource.scoped(scopedF)
      resource
    }
  }

  /** Adds `executeToF` to a `DynamoDBQuery`. */
  implicit class DynamoDBQueryOps[F[_], In, Out](query: DynamoDBQuery[In, Out]) {

    /**
     * Runs this query with the implicit `DynamoDBExceutorF` and returns its result as an `F` effect. See
     * `DynamoDBExceutorF#execute`.
     */
    def executeToF(implicit exF: DynamoDBExceutorF[F], ce: CatsCompatible[Out]): F[ce.Out] =
      exF.execute(query)
  }

  /**
   * Applies function `f: A => DynamoDBQuery[In, B]` to an input stream of `A`, where function f is a write operation
   * ie put or delete. The elements are grouped into chunks of up to 25, and up to `mPar` chunks are processed in
   * parallel. Returns a stream of the results of the writes.
   */
  def batchWriteFromStreamF[F[_], A, In, B](
    fs2StreamIn: fs2.Stream[F, A],
    mPar: Int = 10
  )(
    f: A => DynamoDBQuery[In, B]
  )(implicit
    dynamoDBExceutorF: DynamoDBExceutorF[F],
    async: Async[F],
    d: Dispatcher[F]
  ): fs2.Stream[F, B] = {
    val zioStream: ZStream[Any, Throwable, A] = fs2StreamIn.translate(toZioFunctionK[F]).toZStream()
    val layer                                 = ZLayer.succeed(dynamoDBExceutorF.dynamoDBExecutor)

    val resultZStream = batchWriteFromStream(zioStream, mPar)(f).provideLayer(layer)

    val fs2StreamOut =
      resultZStream.toFs2Stream.translate(CatsCompatible.toCeFunctionK)

    fs2StreamOut
  }

  /**
   * Applies function `pk: A => KeyConditionExpr.PrimaryKeyExpr[From]` to an input stream of `A` and returns a stream of
   * `Either[DynamoDBError.ItemError.DecodingError, (A, Option[From])]` where `From` has an associated Schema. The
   * elements are grouped into chunks of up to 100, and up to `mPar` chunks are processed in parallel. The `Option` is
   * `None` when no item is found for the key.
   */
  def batchReadFromStreamF[F[_], A, From: Schema](
    tableName: String,
    fs2StreamIn: fs2.Stream[F, A],
    mPar: Int = 10
  )(
    pk: A => KeyConditionExpr.PrimaryKeyExpr[From]
  )(implicit
    dynamoDBExceutorF: DynamoDBExceutorF[F],
    async: Async[F],
    d: Dispatcher[F]
  ): fs2.Stream[F, Either[ItemError.DecodingError, (A, Option[From])]] = {

    val zioStream: ZStream[Any, Throwable, A] = fs2StreamIn.translate(toZioFunctionK[F]).toZStream()
    val layer                                 = ZLayer.succeed(dynamoDBExceutorF.dynamoDBExecutor)

    val resultZStream: ZStream[Any, Throwable, Either[ItemError.DecodingError, (A, Option[From])]] =
      batchReadFromStream(tableName, zioStream, mPar)(pk).provideLayer(layer)

    val fs2StreamOut: fs2.Stream[F, Either[ItemError.DecodingError, (A, Option[From])]] =
      resultZStream.toFs2Stream.translate(CatsCompatible.toCeFunctionK)

    fs2StreamOut
  }

  /**
   * Applies function `pk: A => PrimaryKey` to an input stream of `A` and returns a stream of `(A, Option[Item])`.
   * Note this uses the lower level API so returns a type unsafe Item. The elements are grouped into chunks of up to 100,
   * and up to `mPar` chunks are processed in parallel. The `Option` is `None` when no item is found for the key.
   */
  def batchReadItemFromStreamF[F[_], A](
    tableName: String,
    fs2StreamIn: fs2.Stream[F, A],
    mPar: Int = 10
  )(
    pk: A => PrimaryKey
  )(implicit
    dynamoDBExceutorF: DynamoDBExceutorF[F],
    async: Async[F],
    d: Dispatcher[F]
  ): fs2.Stream[F, (A, Option[Item])] = {
    val zioStream: ZStream[Any, Throwable, A] = fs2StreamIn.translate(toZioFunctionK[F]).toZStream()
    val layer                                 = ZLayer.succeed(dynamoDBExceutorF.dynamoDBExecutor)

    val resultZStream: ZStream[Any, Throwable, (A, Option[Item])] =
      batchReadItemFromStream(tableName, zioStream, mPar)(pk).provideLayer(layer)

    val fs2StreamOut: fs2.Stream[F, (A, Option[Item])] =
      resultZStream.toFs2Stream.translate(CatsCompatible.toCeFunctionK)

    fs2StreamOut
  }

}
