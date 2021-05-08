package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.putItem
import zio.dynamodb.ProjectionExpression.$

object PutItemExamples extends App {
  val pi1 = putItem(tableName1, item1) where $("foo.bar") > "1" && $("foo.bar") < "5"
  val pi2 = putItem(tableName2, item2) where $("foo.bar") > "1" || $("foo.bar") < "5"
  val pi3 = putItem(tableName2, item2) where $("foo.bar").isNumber && $("foo.bar").beginsWith("f")

  println(pi1)
  println(pi2)
}
