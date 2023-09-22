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

object syntax {


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

    def of[F[_]: Async](
      commonAwsConfig: config.CommonAwsConfig
    )(implicit zioRuntime: zio.Runtime[Any]): Resource[F, DynamoDBExceutorF[F]] = {
      import zio.interop.catz._

      val ddbLayer = (NettyHttpClient.default ++ ZLayer.succeed(
        commonAwsConfig
      )) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.live

      val scopedF: ZIO[Any with Scope, Throwable, DynamoDBExceutorF[F]] = for {
        zenv <- (ddbLayer >>> DynamoDBExecutor.live).build
      } yield new DynamoDBExceutorF[F](zenv.get[DynamoDBExecutor])
      val resource: Resource[F, DynamoDBExceutorF[F]]                   = Resource.scoped(scopedF)
      resource
    }
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
