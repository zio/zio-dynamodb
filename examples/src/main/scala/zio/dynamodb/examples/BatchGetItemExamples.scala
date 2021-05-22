package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.getItem
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.{ AttrMap, DynamoDBQuery, TableName }

class BatchGetItemExamples {
  val batchManual =
    (getItem(TableName("T1"), AttrMap("field1" -> "1"), $("a.b"), $("c.b")) where $(
      "a.b"
    ) === "X") <*> (getItem(
      TableName("T1"),
      AttrMap("field1" -> "2"),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X") <*> (getItem(
      TableName("T1"),
      AttrMap("field1" -> "3"),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X")

  val batchFromIterable = DynamoDBQuery.forEach(1 to 3) { i =>
    getItem(
      TableName("T1"),
      AttrMap("field1" -> i.toString),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X"
  }
}
