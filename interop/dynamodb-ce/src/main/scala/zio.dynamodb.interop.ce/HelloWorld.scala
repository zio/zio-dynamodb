package zio.dynamodb.interop.ce

import cats.effect.{ IO, IOApp }

object HelloWorld extends IOApp.Simple {
  val run = IO.println("Hello, World!")
}
