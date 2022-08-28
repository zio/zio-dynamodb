package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression._
import zio.dynamodb._

object ConditionExpressionExamples {

  val sizeOnRhs: ConditionExpression[_] = AttributeValue(1) > $("col2").size
  val avToCond1: ConditionExpression[_] = AttributeValue("2") > $("col1")
  val avToCond2: ConditionExpression[_] = AttributeValue("2") < $("col1")

  val exists: ConditionExpression[_]     = $("col1").exists
  val notExists: ConditionExpression[_]  = $("col1").notExists
  val beginsWith: ConditionExpression[_] = $("col1").beginsWith("1")
  val contains: ConditionExpression[_]   = $("col1").contains("1")
  val sizeOnLhs: ConditionExpression[_]  = $("col2").size > 1
  val sizeOnLhs2: ConditionExpression[_] = $("col2").size == 1
  val isType: ConditionExpression[_]     = $("col1").isNumber
  val between: ConditionExpression[_]    = $("col1").between(1, 2)
  val in: ConditionExpression[_]         = $("col1").in(1, 2)
  val expnAnd: ConditionExpression[_]    = beginsWith && sizeOnLhs
  val expnOr: ConditionExpression[_]     = beginsWith || sizeOnLhs
  val expnNot: ConditionExpression[_]    = !beginsWith

  val peNeVal: ConditionExpression[_]   = $("col1") <> 1
  val peLtVal: ConditionExpression[_]   = $("col1") < 1
  val peLtEqVal: ConditionExpression[_] = $("col1") <= 1
  val peGtVal: ConditionExpression[_]   = $("col1") > 1
  val peGtEqVal: ConditionExpression[_] = $("col1") >= 1
  val peCompPe1: ConditionExpression[_] = $("col1") > $("col2")
  val peCompPe2: ConditionExpression[_] = $("col1") === $("col2")

  val peToCond1: ConditionExpression[_]   = $("col1") === "2"
  val peToCond2: ConditionExpression[_]   = $("col1") > 1.0
  val peFnToCond1: ConditionExpression[_] = $("col1").between("1", "2")
  val peFnToCond2: ConditionExpression[_] = $("col1").inSet(Set("1", "2"))
  val peFnToCond3: ConditionExpression[_] = $("col1").in("1", "2")

}
