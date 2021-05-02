package zio.dynamodb.examples

import zio.dynamodb.AttributeValue._
import zio.dynamodb.ProjectionExpression._
import zio.dynamodb._

object ConditionExpressionExamples {

  val str1: String         = String("1")
  val str2: String         = String("2")
  val projectionCol1: Root = Root("col1")
  val projectionCol2: Root = Root("col2")

  // TODO: ungainly ops
  val sizeOnRhs: ConditionExpression = str1 > projectionCol2.size
  val avToCond1: ConditionExpression = String("2") > projectionCol1
  val avToCond2: ConditionExpression = String("2") < projectionCol1

  val exists: ConditionExpression     = projectionCol1.exists
  val notExists: ConditionExpression  = projectionCol1.notExists
  val beginsWith: ConditionExpression = projectionCol1.beginsWith("1")
  val contains: ConditionExpression   = projectionCol1.contains("1")
  val sizeOnLhs: ConditionExpression  = projectionCol2.size > BigDecimal(1.0)
  val isType: ConditionExpression     = projectionCol1.isNumber
  val expnAnd: ConditionExpression    = beginsWith && sizeOnLhs
  val expnOr: ConditionExpression     = beginsWith || sizeOnLhs
  val expnNot: ConditionExpression    = !beginsWith

  val peCompPe1: ConditionExpression = projectionCol1 > projectionCol2
  val peCompPe2: ConditionExpression = projectionCol1 === projectionCol2

  val peToCond1: ConditionExpression   = projectionCol1 === "2"
  val peToCond2: ConditionExpression   = projectionCol1 > "2"
  val peFnToCond1: ConditionExpression = projectionCol1.between("1", "2")
  val peFnToCond2: ConditionExpression = projectionCol1.in(Set("1", "2"))
  val peFnToCond3: ConditionExpression = projectionCol1.in("1", "2")

}
