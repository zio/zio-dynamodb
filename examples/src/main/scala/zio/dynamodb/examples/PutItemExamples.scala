package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.putItem
import zio.dynamodb.ProjectionExpression.$

object PutItemExamples extends App {
  putItem(tableName1, item1) where $("foo.bar") > "1" && $("foo.bar") < "5"
  putItem(tableName2, item2) where $("foo.bar") > "1" || $("foo.bar") < "5"
  putItem(tableName2, item2) where $("foo.bar").isNumber && $("foo.bar").beginsWith("f")

}
