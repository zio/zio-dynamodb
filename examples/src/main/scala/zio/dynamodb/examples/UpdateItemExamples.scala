package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.updateItem
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.{ DynamoDBQuery, Item, PrimaryKey }
import zio.dynamodb.ProjectionExpression.$

object UpdateItemExamples {

  val pi1: DynamoDBQuery[Nothing, Option[Item]] = updateItem("tableName1", PrimaryKey("field1" -> 1)) {
    val expr1            = $("foo.bar").set("a_value")
    // scala 3 needs type hint
    val expr2: Action[_] = expr1 + $("bar.foo").remove
    val expr3            = expr2 + $("foo.foo").appendList(Chunk("s"))
    val expr4            = expr3 + $("foo.foo").prependList(Chunk("s"))
    val expr5            = expr4 + $("baz.fooSet").deleteFromSet(Set(Set("el1")))

    expr5
  } where $("foo.bar") === "value1"
}
