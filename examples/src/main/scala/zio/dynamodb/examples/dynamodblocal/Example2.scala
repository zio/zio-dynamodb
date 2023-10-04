package zio.dynamodb.examples.dynamodblocal

import zio._
import zio.interop.catz._
import zio.stream._
import zio.stream.interop.fs2z._
import cats.effect.IOApp
import cats.effect.IO
import cats.effect.Async
import cats.syntax.all._
import scala.concurrent.Future

object Example2 extends IOApp.Simple {

  val stream: ZStream[Any, Throwable, Int] =
    ZStream(1, 2, 3)

  val zio2: ZIO[Any, Throwable, List[Int]] =
    stream.toFs2Stream.compile.toList

  // Foo.execute interop returns us back the ZIO monad
  // I think we need a CE monad ???
  val x: ZIO[Nothing, Throwable, List[Int]] = Foo.execute(zio2)

  override def run: IO[Unit] =
    for {
      _ <- IO(println("Hello world"))

    } yield ()

  object Foo {
    def execute[F[_], A](zio2: => ZIO[Any, Throwable, A])(implicit F: Async[F]): F[A] =
      F.uncancelable { poll =>
        F.delay(Unsafe.unsafe(implicit u => zio.Runtime.default.unsafe.runToFuture(zio2))).flatMap { future =>
          poll(F.onCancel(F.fromFuture(F.pure[Future[A]](future)), F.fromFuture(F.delay(future.cancel())).void))
        }
      }
  }

}
