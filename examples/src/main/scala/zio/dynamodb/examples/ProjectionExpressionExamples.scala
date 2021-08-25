package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression._

object ProjectionExpressionExamples extends App {

  val top  = Root("top")
  val list = Root("top")(1)
  val map  = Root("top")("1")

  println(list)
  println(map)

  val b1 = ListElement(MapElement(MapElement(Root("foo"), "bar"), "baz"), 9).toString == $("foo.bar.baz[9]").toString
  println(b1)
  val b2 = MapElement(ListElement(Root("foo"), 42), "bar").toString == $("foo[42].bar").toString
  println(b2)
}
