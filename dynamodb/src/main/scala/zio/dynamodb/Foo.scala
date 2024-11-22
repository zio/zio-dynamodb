package zio.dynamodb

object Foo extends App {
  import zio.prelude.data.Optional

  val x: Optional.Present[Int] = Optional.Present(1)
  val y: Optional[Int]         = x.flatMap(v => Some(v + 1))
  println(y)
}
