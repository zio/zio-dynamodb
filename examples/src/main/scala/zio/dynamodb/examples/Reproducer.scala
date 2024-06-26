package zio.dynamodb.examples

import zio.ZLayer
import zio.ZIOAppDefault
import zio.{ Scope, UIO, ZIO, ZIOAppArgs }

object Reproducer extends ZIOAppDefault {

  trait Service {
    def inc(i: Int): UIO[Int]
  }

  class ServiceImpl extends Service { self =>
    def inc(i: Int): UIO[Int] = ZIO.succeed(i + 1)
  }

  val layer = ZLayer.succeed(new ServiceImpl)

  val program                                                             = for {
    service <- ZIO.service[Service]
    i       <- service.inc(1)
    _        = println(s"i=$i")
  } yield ()
  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    program.provideLayer(layer) // compile error here: "local val trace$macro$2 in method run is never used"

}
