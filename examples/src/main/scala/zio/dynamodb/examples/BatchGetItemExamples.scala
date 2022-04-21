package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.getItem
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb._

object BatchGetItemExamples {

  val tableName1: TableName = TableName("T1")
  val tableName2: TableName = TableName("T2")

  /*
/home/avinder/Workspaces/git/zio-dynamodb/examples/src/main/scala/zio/dynamodb/examples/BatchGetItemExamples.scala:16:7
diverging implicit expansion for type zio.dynamodb.ToAttributeValue[A]
starting with value floatSetToAttributeValue in object ToAttributeValue
    ) === "X") <*> (getItem(
   */

  // Queries that are zipped together become eligible for auto-batching in `execute`
  val batchWithZip =
    (getItem("T1", PrimaryKey("field1" -> "1"), $("a.b"), $("c.b")) where $(
      "a.b"
    ) === "X") <*> (getItem(
      "T1",
      PrimaryKey("field1" -> "2"),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X") <*> (getItem(
      "T1",
      PrimaryKey("field1" -> "3"),
      $("a.b"),
      $("c.b")
    ) where $(
      "a.b"
    ) === "X")

  // We can use ZipRight `*>` and ZipLeft `<*` if we wish to ignore the result on one side
  val excludeFromBatchWithZipRight: DynamoDBQuery[(Option[Item], Option[Item])] =
    getItem("T1", PrimaryKey("primaryKey" -> "1"), $("a.b"), $("c.b")) *>
      getItem("T2", PrimaryKey("primaryKey" -> "2"), $("a.b"), $("c.b")) <*>
      getItem("T3", PrimaryKey("primaryKey" -> "3"), $("a.b"), $("c.b"))

  // If we have an Iterable of data from which we wish to create a batch query from we can use `DynamoDBQuery.forEach`
  // The below example will create 1 BatchGetItem containing 10 GetItem requests
  val batchFromIterable                                                         = DynamoDBQuery.forEach(1 to 10) { i =>
    getItem(
      "T1",
      PrimaryKey("field1" -> i),
      $("field1"),
      $("field2")
    ) where $(
      "field1"
    ) === 42
  }
}
