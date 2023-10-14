package zio.dynamodb.interop.ce

import zio._
import zio.interop.catz._
import zio.stream._
import zio.stream.interop.fs2z._

object Example {

  val stream: ZStream[Any, Throwable, Int] =
    ZStream(1, 2, 3)
  
  val zio: ZIO[Any, Throwable, List[Int]] = 
    stream.toFs2Stream.compile.toList
}
