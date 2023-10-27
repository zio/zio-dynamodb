package zio.dynamodb.interop.ce

import zio.dynamodb.DynamoDBQuery
import cats.effect.Async
import cats.syntax.all._

import zio.aws.dynamodb

import zio.Unsafe
import scala.concurrent.Future
import zio.dynamodb.DynamoDBExecutor
import zio.aws.core.config
import cats.effect.kernel.Resource
import zio.{ Scope, ZIO }
import zio.aws.netty.NettyHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import zio.stream.ZStream
import zio.dynamodb.KeyConditionExpr
import zio.schema.Schema
import zio.dynamodb.DynamoDBError
import cats.arrow.FunctionK
import cats.effect.std.Dispatcher
import zio.dynamodb.batchReadFromStream
import zio.ZLayer

object syntax2 {

  trait CatsCompatible[ZioType] {
    type Out

    def toCats(a: ZioType): Out
  }

  final def toZioFunctionK[F[_]](implicit d: Dispatcher[F]): FunctionK[F, zio.Task] =
    new FunctionK[F, zio.Task] {
      def apply[A](t: F[A]): zio.Task[A] = toZioEffect(t)
    }

  /*
  TODO:
  1)  go from F -> Future using Dispatcher
  2)  go from Future -> ZIO via ZIO.fromFuture
   */
  def toZioEffect[F[_], A](t: F[A])(implicit d: Dispatcher[F]): zio.Task[A] =
    ZIO.uninterruptibleMask { restore =>
      val future = d.unsafeToFuture(t)
      val zio    = ZIO.fromFuture(_ => future)
      restore(zio)
    }

  object CatsCompatible extends CatsCompatibleLowPriority {
    type Aux[A, Out0] = CatsCompatible[A] { type Out = Out0 }
    import zio.stream.interop.fs2z._
    import cats.arrow.FunctionK

    // we need to make this generic in F so that it can be used by batchReadFromStreamF
    // which is generic in F
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

    // this could be generalaised to any F maybe with
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

    def default[F[_]: Async]: Resource[F, DynamoDBExceutorF[F]] = ofCustomised(identity)

    // clean up was done
    // we only expose AWS SDK API - not zio.aws API so no zio.aws imports needed
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

  // TODO: do we need this ???
  implicit class DynamoDBQueryOps[F[_], In, Out](query: DynamoDBQuery[In, Out]) {

    def executeToF(implicit exF: DynamoDBExceutorF[F], ce: CatsCompatible[Out]): F[ce.Out] =
      exF.execute(query)
  }

  /*
  we have the option of
  - re-writing the util function in CE - an using new interop above
  - doinging FS2 -> ZStream conversion and calling original ZIO function

  def batchReadFromStream[R, A, From: Schema](
    tableName: String,
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(
    pk: A => KeyConditionExpr.PrimaryKeyExpr[From]
  ): ZStream[R with DynamoDBExecutor, Throwable, Either[DynamoDBError.DecodingError, (A, Option[From])]] =
   */
  def batchReadFromStreamF[F[_], A, From: Schema](
    tableName: String,
    fs2StreamIn: fs2.Stream[F, A], // need conversion here
    mPar: Int = 10
  )(
    pk: A => KeyConditionExpr.PrimaryKeyExpr[From]
  )(implicit
    dynamoDBExceutorF: DynamoDBExceutorF[F],
    async: Async[F],
    d: Dispatcher[F]
  ): fs2.Stream[F, Either[DynamoDBError.DecodingError, (A, Option[From])]] = {
    import zio.stream.interop.fs2z._
    // fs2Stream -> ZIOStream
    val zioStream: ZStream[Any, Throwable, A] = fs2StreamIn.translate(toZioFunctionK[F]).toZStream()

    val layer = ZLayer.succeed(dynamoDBExceutorF.dynamoDBExecutor)

    val resultZStream: ZStream[Any, Throwable, Either[DynamoDBError.DecodingError, (A, Option[From])]] =
      batchReadFromStream(tableName, zioStream, mPar)(pk).provideLayer(layer) // TODO: review

    val fs2StreamOut: fs2.Stream[F, Either[DynamoDBError.DecodingError, (A, Option[From])]] =
      resultZStream.toFs2Stream.translate(CatsCompatible.toCeFunctionK)

    //.toFs2Stream.translate(CatsCompatible.toCeFunctionK)
    fs2StreamOut
  }

}
