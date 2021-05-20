package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.getItem
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.{ AttributeValue, DynamoDBQuery, PrimaryKey, TableName }

class BatchGetItemExamples {
  val batchManual =
    (getItem(TableName("T1"), PrimaryKey(Map("field1" -> AttributeValue.String("1"))), $("a.b"), $("c.b")) where $(
      "a.b"
    ) === "X") <*> (getItem(
      TableName("T1"),
      PrimaryKey(Map("field1" -> AttributeValue.String("2"))),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X") <*> (getItem(
      TableName("T1"),
      PrimaryKey(Map("field1" -> AttributeValue.String("3"))),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X")

  val batchFromIterable = DynamoDBQuery.forEach(1 to 3) { i =>
    getItem(
      TableName("T1"),
      PrimaryKey(Map("field1" -> AttributeValue.String(i.toString))),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X"
  }
}
