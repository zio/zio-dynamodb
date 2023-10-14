package zio.dynamodb.interop.ce

import fs2.Pure
import zio.stream.ZStream
import cats.effect.kernel.Async
import cats.arrow.FunctionK
//import cats.syntax.all._

class Foo[F[_]]() {
  import zio.stream.interop.fs2z._

  val fs2Stream: fs2.Stream[Pure, Int]        = fs2.Stream(1, 2, 3)
  val zioStream: ZStream[Any, Throwable, Int] = fs2Stream.toZStream()

  val fs2Stream2: fs2.Stream[cats.effect.IO, Int] = ???
  val x: ZStream[Any, Throwable, Int]             = fs2Stream.toZStream()

  
  val zioStream2: ZStream[Any, Throwable, Int]    = fs2Stream2.translate(Foo.toZioFunctionK).toZStream()

}

trait FooBar[F[_], A] {
  def toZio(a: F[A]): zio.Task[A] = ???  
}

// I think we need a type class on F
object Foo {
  def fromEffect[F[_], A](from: F[A])(implicit F: Async[F]): zio.Task[A] = ???

  val toZioFunctionK: FunctionK[cats.effect.IO, zio.Task] =
    new FunctionK[cats.effect.IO, zio.Task] {
      def apply[A](t: cats.effect.IO[A]): zio.Task[A] = fromEffect[cats.effect.IO, A](t)
    }

}
