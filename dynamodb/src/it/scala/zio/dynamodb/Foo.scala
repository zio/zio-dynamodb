package zio.dynamodb

import zio.ZIO

object Foo {
  val list = List(ZIO.unit, ZIO.unit, ZIO.unit)

  ZIO.foreach(list)(identity)
}
