package zio.dynamodb.examples

import zio.dynamodb.AttributeValue._
import zio.dynamodb.ProjectionExpression._
import zio.dynamodb._

object ConditionExpressionExamples {

  // TODO: improve ergonomics for conditions with AttributeValue on the LHS
  val sizeOnRhs: ConditionExpression = Number(1) > $("col2").size
  val avToCond1: ConditionExpression = String("2") > $("col1")
  val avToCond2: ConditionExpression = String("2") < $("col1")

  val exists: ConditionExpression     = $("col1").exists
  val notExists: ConditionExpression  = $("col1").notExists
  val beginsWith: ConditionExpression = $("col1").beginsWith("1")
  val contains: ConditionExpression   = $("col1").contains("1")
  val sizeOnLhs: ConditionExpression  = $("col2").size > 1
  val isType: ConditionExpression     = $("col1").isNumber
  val expnAnd: ConditionExpression    = beginsWith && sizeOnLhs
  val expnOr: ConditionExpression     = beginsWith || sizeOnLhs
  val expnNot: ConditionExpression    = !beginsWith

  val peCompPe1: ConditionExpression = $("col1") > $("col2")
  val peCompPe2: ConditionExpression = $("col1") === $("col2")

  val peToCond1: ConditionExpression   = $("col1") === "2"
  val peToCond2: ConditionExpression   = $("col1") > 1.0
  val peFnToCond1: ConditionExpression = $("col1").between("1", "2")
  val peFnToCond2: ConditionExpression = $("col1").in(Set("1", "2"))
  val peFnToCond3: ConditionExpression = $("col1").in("1", "2")

}
