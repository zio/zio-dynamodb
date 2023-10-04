package zio.dynamodb.examples.dynamodblocal

import zio._
import zio.interop.catz._
import zio.stream._
import zio.stream.interop.fs2z._
import fs2.Pure
import cats.Id

object Example {

  val fs2Stream: fs2.Stream[Pure, Int] = fs2.Stream(1, 2, 3)
  val fs2StreamCompiled: Id[List[Int]] = fs2Stream.map(_ + 1).compile.toList

  val stream: ZStream[Any, Throwable, Int] = ZStream(1, 2, 3)
  val zio: ZIO[Any, Throwable, List[Int]]  = stream.toFs2Stream.compile.toList
}
