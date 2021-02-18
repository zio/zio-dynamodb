package zio.dynamodb.examples

import zio.dynamodb._
import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.dynamodb.ProjectionExpression._

object ConditionExpressionExamples {

  val x: ConditionExpression = ValueOperand(AttributeValue.String("")) == ValueOperand(AttributeValue.String(""))
  val y: ConditionExpression = x && x

  val p: ConditionExpression =
    PathOperand(TopLevel("foo")(1)) > ValueOperand(AttributeValue.Number(1.0)) // TODO: infix ops require brackets

  val c    = AttributeType(TopLevel("foo")(1), AttributeValueType.Number) && p
  val notC = !c
}
