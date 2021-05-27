package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.TestData.tableName1
import zio.dynamodb.DynamoDBQuery.{ deleteItem, putItem }
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb._

class BatchWriteItemExamples {
  val table1 = TableName("T1")
  val table2 = TableName("T2")

  val batchManual =
    (putItem(table1, Item("field1" -> 1)) where $("a.b") === "1") <*> deleteItem(
      table2,
      PrimaryKey("primaryKey" -> 1)
    ) where $("c.b") === "2"

  val batchPutFromIterable = DynamoDBQuery.forEach(1 to 3) { i =>
    putItem(table1, Item("field1" -> i.toString))
  }

  val batchDeleteFromIterable = DynamoDBQuery.forEach(1 to 3) { i =>
    deleteItem(tableName1, PrimaryKey("pk" -> i.toString)) where $("foo.bar") > "1" && !($("foo.bar") < "5")
  }

}
