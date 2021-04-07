package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, GetItem }
import zio.dynamodb.{ AttributeValue, PrimaryKey, TableName }

class BatchGetItemExamples {
  val pk1    = PrimaryKey(Map("field1" -> AttributeValue.Number(1.0)))
  val table1 = TableName("T1")
  val table2 = TableName("T2")
  val batch  = BatchGetItem().addAll(GetItem(table1, pk1), GetItem(table2, pk1))
}
