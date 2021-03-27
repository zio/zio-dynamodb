package zio.dynamodb

import zio.ZIO
import zio.stream.ZStream
import zio.test.{ DefaultRunnableSpec, _ }

//noinspection TypeAnnotation
object StreamingSpec extends DefaultRunnableSpec {

  override def spec = streamingSuite

  val stream1 = ZStream.fromEffect(ZIO {
    println("inside stream")
    1
  })

  def execute: ZIO[Any, Throwable, (ZStream[Any, Throwable, Int], Option[Int])] =
    ZIO.effect((stream1, Some(1)))

  // does this result in resource leaks for stream1 ?
  val streamingSuite = suite("Streaming interface experiment")(
    testM("should access non stream tuple item without pulling on stream") {
      for {
        tuple <- execute
        _     <- ZIO(println(tuple._2)) // "inside stream" is not printed
      } yield assertCompletes
    }
  )

}
