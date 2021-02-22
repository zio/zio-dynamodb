package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb.UpdateExpression.Action.{ RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand.ValueOperand
import zio.dynamodb.{ AttributeValue, UpdateExpression }

object UpdateExpressionExamples extends App {

  val sa1: SetAction    = SetAction(TopLevel("top")(1), ValueOperand(AttributeValue.Number(1.0)))
  val sa2: SetAction    = SetAction(TopLevel("top")(2), ValueOperand(AttributeValue.Number(2.0)))
  val sa3: SetAction    = SetAction(TopLevel("top")(3), ValueOperand(AttributeValue.Number(3.0)))
  val ra1: RemoveAction = RemoveAction(TopLevel("top")(1))

  val exprn1 = UpdateExpression(sa1)
  val exprn2 = exprn1 + sa2
  val exprn3 = (exprn2 + sa2) + ra1

  println(exprn3)
  private val x = exprn3.grouped
  println(x)
}
