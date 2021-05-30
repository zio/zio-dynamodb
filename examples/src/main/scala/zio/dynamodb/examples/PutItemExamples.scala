package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.putItem
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb._

object PutItemExamples extends App {
  putItem("tableName1", Item("field1" -> 1)) where $("foo.bar") > "1" && $("foo.bar") < "5"
  putItem("tableName2", Item("field1" -> 1)) where $("foo.bar") > "1" || $("foo.bar") < "5"
  putItem("tableName2", Item("field1" -> 1)) where $("foo.bar").isNumber && $("foo.bar").beginsWith("f")

}
