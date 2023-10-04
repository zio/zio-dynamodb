package zio.dynamodb.interop.ce

import zio.dynamodb.DynamoDBQuery
import cats.effect.Async
import cats.syntax.all._

import zio.aws.dynamodb

import zio.Unsafe
import scala.concurrent.Future
import zio.dynamodb.DynamoDBExecutor
import zio.aws.core.config
// import zio.aws.core.httpclient.HttpClient
// import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import cats.effect.kernel.Resource
import zio.ZLayer
import zio.{ Scope, ZIO }
import zio.aws.netty.NettyHttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import zio.stream.ZStream

object syntax {
  /*
  ZIO Type determins the Cats type we get from the conversion process
   */
  trait CatsCompatible[ZioType] {
    type Out // Cats type

    def toCats(a: ZioType): Out
  }

  object CatsCompatible extends CatsCompatibleLowPriority {
    type Aux[A, Out0] = CatsCompatible[A] { type Out = Out0 }
    import zio.stream.interop.fs2z._

    implicit def zioStreamCatsCompatible[A]
      : CatsCompatible.Aux[ZStream[Any, Throwable, A], fs2.Stream[cats.effect.IO, A]] =
      new CatsCompatible[ZStream[Any, Throwable, A]] {
        type Out = fs2.Stream[cats.effect.IO, A]

        def toCats(a: ZStream[Any, Throwable, A]): Out = {

          import cats.arrow.FunctionK
          val first: FunctionK[List, Option]            = new FunctionK[List, Option] {
            def apply[A](l: List[A]): Option[A] = l.headOption
          }
          val fooK: FunctionK[zio.Task, cats.effect.IO] =
            new FunctionK[zio.Task, cats.effect.IO] {
              def apply[A](t: zio.Task[A]): cats.effect.IO[A] = toEffect[cats.effect.IO, A](t)
            }

          val y: fs2.Stream[cats.effect.IO, A] = a.toFs2Stream.translate(fooK)
          y
        }
      }
  }

  def toEffect[F[_], A](t: zio.Task[A])(implicit F: Async[F]): F[A] =
    F.uncancelable { poll =>
      F.delay(
        Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.runToFuture(t))
      ).flatMap { future =>
        poll(F.onCancel(F.fromFuture(F.pure[Future[A]](future)), F.fromFuture(F.delay(future.cancel())).void))
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

    // TODO: do we need this?
    def of2[F[_]](dynamoDBExecutor: DynamoDBExecutor)(implicit F: Async[F]): DynamoDBExceutorF[F] =
      new DynamoDBExceutorF[F](dynamoDBExecutor)

    // another overlaed method without customization

    // TODO: use default Runtime
    def of[F[_]: Async](
      commonAwsConfig: config.CommonAwsConfig
    )(
      customization: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
    ): Resource[F, DynamoDBExceutorF[F]] = {
      import zio.interop.catz._

      implicit val runtime = zio.Runtime.default

      val ddbLayer = (NettyHttpClient.default ++ ZLayer.succeed(
        commonAwsConfig
      )) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized(customization)

      val scopedF: ZIO[Any with Scope, Throwable, DynamoDBExceutorF[F]] = for {
        zenv <- (ddbLayer >>> DynamoDBExecutor.live).build
      } yield new DynamoDBExceutorF[F](zenv.get[DynamoDBExecutor])
      val resource: Resource[F, DynamoDBExceutorF[F]]                   = Resource.scoped(scopedF)
      resource
    }
  }

  // questionable benfit ???
  implicit class DynamoDBQueryOps[F[_], In, Out](query: DynamoDBQuery[In, Out]) {

    def executeToF(implicit exF: DynamoDBExceutorF[F], ce: CatsCompatible[Out]): F[ce.Out] =
      exF.execute(query)
  }

  // implicit class DynamoDBQueryOps[In, Out](query: DynamoDBQuery[In, Out]) {

  //   def executeToF[F[_]](implicit F: Async[F]): F[Out] =
  //     F.uncancelable { poll =>
  //       F.delay(Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.runToFuture(dynamoDBExector.execute(query))))
  //         .flatMap { future =>
  //           poll(F.onCancel(F.fromFuture(F.pure[Future[Out]](future)), F.fromFuture(F.delay(future.cancel())).void))
  //         }
  //     }

  // }

}
