package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.TestData.{ primaryKey1, tableName1 }
import zio.dynamodb.DynamoDBQuery.updateItem
import zio.dynamodb.ProjectionExpression.$

object UpdateItemExamples {

  val pi1 = updateItem(tableName1, primaryKey1, $("foo.bar").set("a_value"))
}
