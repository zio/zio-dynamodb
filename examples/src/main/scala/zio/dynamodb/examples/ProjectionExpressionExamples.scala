package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression._

object ProjectionExpressionExamples extends App {

  val top  = Root("top")
  val list = Root("top")(1)
  val map  = Root("top")("1")

  println(list)
  println(map)

  val b1 = ListElement(MapElement(MapElement(Root("foo"), "bar"), "baz"), 9) === $("foo.bar.baz[9]")
  println(b1)
  val b2 = MapElement(ListElement(Root("foo"), 42), "bar") === $("foo[42].bar")
  println(b2)

  val peNeVal   = $("col1") <> 1
  val peLtVal   = $("col1") < 1
  val peLtEqVal = $("col1") <= 1
  val peGtVal   = $("col1") > 1
  val peGtEqVal = $("col1") >= 1
  val peCompPe1 = $("col1") > $("col2")
  val peCompPe2 = $("col1") === $("col2")
}
