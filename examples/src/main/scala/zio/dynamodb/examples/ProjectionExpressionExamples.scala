package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression
import zio.dynamodb.ProjectionExpression._

object ProjectionExpressionExamples extends App {

  val top                        = Root("top")
  val list: ProjectionExpression = Root("top")(1)
  val map                        = Root("top")("1")

  println(list)
  println(map)
}
