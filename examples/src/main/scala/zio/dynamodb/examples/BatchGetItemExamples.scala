package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.BatchGetItem
import zio.dynamodb.{ AttributeValue, PrimaryKey, TableName }

class BatchGetItemExamples {
  val pk1    = PrimaryKey(Map("field1" -> AttributeValue.Number(1.0)))
  val table1 = TableName("T1")
  val table2 = TableName("T2")
  val batch  = BatchGetItem(Map(table1 -> BatchGetItem.TableItem(pk1), table2 -> BatchGetItem.TableItem(pk1)))
}
