package zio.dynamodb.examples

import zio.dynamodb.ProjectionExpression.$

object BatchGetItemExamples2 {

  val batchWithZip = $("a.b") === "X"
}
