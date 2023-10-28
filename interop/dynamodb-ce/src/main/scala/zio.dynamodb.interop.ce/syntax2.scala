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

object syntax2 {

  trait CatsCompatible[ZioType] {
    type Out

    def toCats(a: ZioType): Out
  }

  object CatsCompatible extends CatsCompatibleLowPriority {
    type Aux[A, Out0] = CatsCompatible[A] { type Out = Out0 }
    import zio.stream.interop.fs2z._
    import cats.arrow.FunctionK

    // final private val toCeFunctionK: FunctionK[zio.Task, cats.effect.IO] =
    //   new FunctionK[zio.Task, cats.effect.IO] {
    //     def apply[A](t: zio.Task[A]): cats.effect.IO[A] = toEffect[cats.effect.IO, A](t)
    //   }
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

    // implicit def zioStreamCatsCompatible[A]
    //   : CatsCompatible.Aux[ZStream[Any, Throwable, A], fs2.Stream[cats.effect.IO, A]] =
    //   new CatsCompatible[ZStream[Any, Throwable, A]] {
    //     type Out = fs2.Stream[cats.effect.IO, A]

    //     def toCats(a: ZStream[Any, Throwable, A]): Out = a.toFs2Stream.translate(toCeFunctionK)
    //   }
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

  class DynamoDBExceutorF[F[_]](dynamoDBExecutor: DynamoDBExecutor)(implicit F: Async[F]) {
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

    def ofCustomised[F[_]: Async](
      customization: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder
    ): Resource[F, DynamoDBExceutorF[F]] = {
      import zio.interop.catz._

      implicit val runtime = zio.Runtime.default

      val ddbLayer =
        NettyHttpClient.default >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized(customization)

      val scopedF: ZIO[Scope, Throwable, DynamoDBExceutorF[F]] = for {
        zenv <- (ddbLayer >>> DynamoDBExecutor.live).build
      } yield new DynamoDBExceutorF[F](zenv.get[DynamoDBExecutor])
      val resource: Resource[F, DynamoDBExceutorF[F]]          = Resource.scoped(scopedF)
      resource
    }
  }

  implicit class DynamoDBQueryOps[F[_], In, Out](query: DynamoDBQuery[In, Out]) {

    def executeToF(implicit exF: DynamoDBExceutorF[F], ce: CatsCompatible[Out]): F[ce.Out] =
      exF.execute(query)
  }

}
