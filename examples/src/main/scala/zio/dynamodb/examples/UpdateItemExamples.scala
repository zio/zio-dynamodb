package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.updateItem
import zio.dynamodb.PrimaryKey
import zio.dynamodb.ProjectionExpression.$

object UpdateItemExamples {

  val pi1 = updateItem("tableName1", PrimaryKey("field1" -> 1)) {
    $("foo.bar").set("a_value") +
      $("bar.foo").remove +
      $("foo.foo").setListAppend(Chunk("s")) +
      $("foo.foo").setListPrepend(Chunk("s")) +
      $("baz.fooSet").deleteFromSet("el1")
  } where $("foo.bar") === "value1"
}
