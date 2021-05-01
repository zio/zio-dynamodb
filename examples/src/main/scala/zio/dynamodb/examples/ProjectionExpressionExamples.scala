package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression._

object ProjectionExpressionExamples {

  val top  = Root("top")
  val list = Root("top")(1)
  val map  = Root("top")("1")
}
