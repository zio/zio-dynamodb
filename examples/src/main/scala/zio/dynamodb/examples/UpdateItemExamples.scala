package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.DynamoDBExecutor.TestData.{ primaryKey1, tableName1 }
import zio.dynamodb.DynamoDBQuery.updateItem
import zio.dynamodb.ProjectionExpression.$

object UpdateItemExamples {

  val pi1 = updateItem(tableName1, primaryKey1) {
    $("foo.bar").set("a_value") +
      $("bar.foo").remove +
      $("foo.foo").setListAppend(Chunk("s")) +
      $("foo.foo").setListPrepend(Chunk("s")) +
      $("baz.fooSet").deleteFromSet("el1")
  } where $("foo.bar") === "value1"
}
