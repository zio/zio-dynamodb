package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.deleteItem
import zio.dynamodb.PrimaryKey
import zio.dynamodb.ProjectionExpression.$

object DeleteItemExamples extends App {
  deleteItem(tableName1, PrimaryKey("field1" -> 1)) where $("foo.bar") > "1" && !($("foo.bar") < "5")
  deleteItem(tableName2, PrimaryKey("field1" -> 1)) where $("foo.bar").between("a", "b")
}
