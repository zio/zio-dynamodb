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

object syntax {

  /*
  DI stories in CE
  https://medium.com/@ivovk/dependency-injection-with-cats-effect-resource-monad-ad7cd47b977
   */

  class DynamoDBExceutorF[F[_]](dynamoDBExecutor: DynamoDBExecutor)(implicit F: Async[F]) {
    def execute[Out](query: DynamoDBQuery[_, Out]): F[Out] =
      F.uncancelable { poll =>
        F.delay(Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.runToFuture(dynamoDBExecutor.execute(query))))
          .flatMap { future =>
            poll(F.onCancel(F.fromFuture(F.pure[Future[Out]](future)), F.fromFuture(F.delay(future.cancel())).void))
          }
      }
  }

  object DynamoDBExceutorF {
    def of2[F[_]](dynamoDBExecutor: DynamoDBExecutor)(implicit F: Async[F]): DynamoDBExceutorF[F] =
      new DynamoDBExceutorF[F](dynamoDBExecutor)
    /*
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }


  def scoped[F[_]: Async, R, A](
    zio: ZIO[Scope with R, Throwable, A]
  )(implicit runtime: Runtime[R], trace: Trace): Resource[F, A] =
    scopedZIO[R, Throwable, A](zio).mapK(new (ZIO[R, Throwable, _] ~> F) {
      override def apply[B](zio: ZIO[R, Throwable, B]) = toEffect(zio)
    })

     */

    implicit val r: zio.Runtime[Any] = ???

    def of[F[_]: Async](
      awsConfig: config.AwsConfig
    ): Resource[F, DynamoDBExceutorF[F]] = {
      import zio.interop.catz._

      // use interop to convert ZIO -> Resource
      // map over that into ExcecutorF
      // create one of these using of passing in config
      val scopedF: ZIO[Any with Scope, Throwable, DynamoDBExceutorF[F]] = for {
        zenv <- (ZLayer.succeed(awsConfig) >>> dynamodb.DynamoDb.live >>> DynamoDBExecutor.live).build
      } yield new DynamoDBExceutorF[F](zenv.get[DynamoDBExecutor])
      val resource: Resource[F, DynamoDBExceutorF[F]]                   = Resource.scoped(scopedF)
      resource
    }
  }

  implicit class DynamoDBQueryOps[In, Out](query: DynamoDBQuery[In, Out]) {

    // def executeToF[F[_]](implicit F: Async[F]): F[Out] =
    //   F.uncancelable { poll =>
    //     F.delay(Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.runToFuture(dynamoDBExector.execute(query))))
    //       .flatMap { future =>
    //         poll(F.onCancel(F.fromFuture(F.pure[Future[Out]](future)), F.fromFuture(F.delay(future.cancel())).void))
    //       }
    //   }

  }

}
