package zio.dynamodb.examples

import zio.dynamodb.AttrMap
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.deleteItem
import zio.dynamodb.ProjectionExpression.$

object DeleteItemExamples extends App {
  deleteItem(tableName1, AttrMap.empty) where $("foo.bar") > "1" && !($("foo.bar") < "5")
  deleteItem(tableName2, AttrMap.empty) where $("foo.bar").between("a", "b")
}
