package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression._

object ProjectionExpressionExamples {

  val top  = TopLevel("top")
  val list = TopLevel("top")(1)
  val map  = TopLevel("top")("1")
}
