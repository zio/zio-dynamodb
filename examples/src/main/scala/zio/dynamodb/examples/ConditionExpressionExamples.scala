package zio.dynamodb.examples

import zio.dynamodb.AttributeValue._
import zio.dynamodb.ProjectionExpression._
import zio.dynamodb._

object ConditionExpressionExamples {

  val str1: String                     = String("1")
  val str2: String                     = String("2")
  val greaterThan: ConditionExpression = str1.operand > str2.operand
  val between: ConditionExpression     = str1.operand.between(str1, str2)
  val projectionCol1: TopLevel         = TopLevel("col1")
  val projectionCol2: TopLevel         = TopLevel("col2")
  val exists: ConditionExpression      = projectionCol1.exists
  val notExists: ConditionExpression   = projectionCol1.notExists
  val beginsWith: ConditionExpression  = projectionCol1.beginsWith(str1)
  val contains: ConditionExpression    = projectionCol1.contains(str1)
  val size: ConditionExpression        = projectionCol2.size > Number(1.0).operand
  val isType: ConditionExpression      = projectionCol1.isType(AttributeValueType.Number)
  val expnAnd: ConditionExpression     = beginsWith && size
  val expnOr: ConditionExpression      = beginsWith || size
  val expnNot: ConditionExpression     = !beginsWith

}
