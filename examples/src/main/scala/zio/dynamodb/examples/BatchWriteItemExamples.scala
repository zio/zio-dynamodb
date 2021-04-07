package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.{ BatchWriteItem, DeleteItem, PutItem }
import zio.dynamodb._

class BatchWriteItemExamples {
  val pk1    = PrimaryKey(Map("field1" -> AttributeValue.Number(1.0)))
  val item1  = Item(Map("field1" -> AttributeValue.Number(1.0)))
  val item2  = Item(Map("field2" -> AttributeValue.Number(2.0)))
  val table1 = TableName("T1")
  val table2 = TableName("T2")
  val batch  = BatchWriteItem().addAll(PutItem(table1, item1), PutItem(table1, item2), DeleteItem(table2, pk1))
}
