package zio.dynamodb.interop.ce

import zio.dynamodb.DynamoDBQuery
import cats.effect.Async
import cats.syntax.all._

import zio.Unsafe
import scala.concurrent.Future
import zio.dynamodb.DynamoDBExecutor

object syntax {

  /*
  DI stories in CE
  https://medium.com/@ivovk/dependency-injection-with-cats-effect-resource-monad-ad7cd47b977
   */
  private lazy val dynamoDBExector: DynamoDBExecutor = ???
  // (implicit dynamoDBExector: DynamoDBExecutor)


  implicit class DynamoDBQueryOps[In, Out](query: DynamoDBQuery[In, Out]) {

    def executeToF[F[_]](implicit F: Async[F]): F[Out] =
      F.uncancelable { poll =>
        F.delay(Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.runToFuture(dynamoDBExector.execute(query))))
          .flatMap { future =>
            poll(F.onCancel(F.fromFuture(F.pure[Future[Out]](future)), F.fromFuture(F.delay(future.cancel())).void))
          }
      }

  }

}
