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
import zio.dynamodb._
import zio.schema.Schema
import zio.stream.ZStream
import zio.stream.interop.fs2z._

import scala.concurrent.Future

object syntax {

  trait CatsCompatible[ZioType] {
    type Out

    def toCats(a: ZioType): Out
  }

  def toZioFunctionK[F[_]](implicit d: Dispatcher[F]): FunctionK[F, zio.Task] =
    new FunctionK[F, zio.Task] {
      def apply[A](t: F[A]): zio.Task[A] = toZioEffect(t)
    }

  def toZioEffect[F[_], A](t: F[A])(implicit d: Dispatcher[F]): zio.Task[A] =
    ZIO.uninterruptibleMask { restore =>
      val future = d.unsafeToFuture(t)
      val zio    = ZIO.fromFuture(_ => future)
      restore(zio)
    }

  object CatsCompatible extends CatsCompatibleLowPriority {
    type Aux[A, Out0] = CatsCompatible[A] { type Out = Out0 }

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

    implicit def zioStreamCatsCompatible[F[_], A](implicit
      F: Async[F]
    ): CatsCompatible.Aux[ZStream[Any, Throwable, A], fs2.Stream[F, A]] =
      new CatsCompatible[ZStream[Any, Throwable, A]] {
        type Out = fs2.Stream[F, A]

        def toCats(a: ZStream[Any, Throwable, A]): Out = a.toFs2Stream.translate(toCeFunctionK)
      }
  }

  trait CatsCompatibleLowPriority {
    implicit def catsCompatible[A]: CatsCompatible.Aux[A, A] =
      new CatsCompatible[A] {
        type Out = A

        def toCats(a: A): Out = a
      }
  }

  class DynamoDBExceutorF[F[_]](val dynamoDBExecutor: DynamoDBExecutor)(implicit F: Async[F]) {
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

  object DynamoDBExceutorF {

    def of[F[_]: Async]: Resource[F, DynamoDBExceutorF[F]] = ofCustomised(identity)

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

  implicit class DynamoDBQueryOps[F[_], In, Out](query: DynamoDBQuery[In, Out]) {

    def executeToF(implicit exF: DynamoDBExceutorF[F], ce: CatsCompatible[Out]): F[ce.Out] =
      exF.execute(query)
  }

  /**
   * Applies function `f: A => DynamoDBQuery[In, B]` to an input stream of `A`, where function f is a write operation
   * ie put or delete
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
   * `Either[DynamoDBError.DecodingError, (A, Option[From])]` where `From` has an associated Schema
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
  ): fs2.Stream[F, Either[DynamoDBError.DecodingError, (A, Option[From])]] = {

    val zioStream: ZStream[Any, Throwable, A] = fs2StreamIn.translate(toZioFunctionK[F]).toZStream()
    val layer                                 = ZLayer.succeed(dynamoDBExceutorF.dynamoDBExecutor)

    val resultZStream: ZStream[Any, Throwable, Either[DynamoDBError.DecodingError, (A, Option[From])]] =
      batchReadFromStream(tableName, zioStream, mPar)(pk).provideLayer(layer)

    val fs2StreamOut: fs2.Stream[F, Either[DynamoDBError.DecodingError, (A, Option[From])]] =
      resultZStream.toFs2Stream.translate(CatsCompatible.toCeFunctionK)

    fs2StreamOut
  }

  /**
   * Applies function `pk: A => PrimaryKey` to an input stream of `A` and returns a stream of `(A, Option[Item])`.
   * Note this uses the lower level API so returns a type unsafe Item.
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
