package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.updateItem
import zio.dynamodb.{ DynamoDBQuery, Item, PrimaryKey }
import zio.dynamodb.ProjectionExpression.$

object UpdateItemExamples {

  val pi1: DynamoDBQuery[_, Option[Item]] = updateItem("tableName1", PrimaryKey("field1" -> 1)) {
    $("foo.bar").set("a_value") +
      $("bar.foo").remove +
      $("foo.foo").appendList(Chunk("s")) +
      $("foo.foo").prependList(Chunk("s")) +
      $("baz.fooSet").deleteFromSet(Set(Set("el1")))
  } where $("foo.bar") === "value1"
}
