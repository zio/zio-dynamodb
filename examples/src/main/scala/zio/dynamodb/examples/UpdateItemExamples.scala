package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.updateItem
import zio.dynamodb.PrimaryKey
import zio.dynamodb.ProjectionExpression.$

object UpdateItemExamples {

  val pi1 = updateItem("tableName1", PrimaryKey("field1" -> 1)) {
    $("foo.bar").setX("a_value") +
      $("bar.foo").remove +
      $("foo.foo").appendList(Chunk("s")) +
      $("foo.foo").prependList(Chunk("s")) +
      $("baz.fooSet").deleteFromSet("el1")
  } where $("foo.bar") === "value1"
}
