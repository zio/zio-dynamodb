package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery.BatchWriteItem
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.WriteItemsMap
import zio.dynamodb.{ AttributeValue, Item, PrimaryKey, TableName }

class BatchWriteItemExamples {
  val pk1    = PrimaryKey(Map("field1" -> AttributeValue.Number(1.0)))
  val item1  = Item(Map("field1" -> AttributeValue.Number(1.0)))
  val item2  = Item(Map("field2" -> AttributeValue.Number(2.0)))
  val table1 = TableName("T1")
  val table2 = TableName("T2")
  val batch  = BatchWriteItem(
    WriteItemsMap(
      Map(
        table1 -> Set(BatchWriteItem.Put(item1)),
        table1 -> Set(BatchWriteItem.Put(item2)),
        table2 -> Set(BatchWriteItem.Delete(pk1))
      )
    )
  )
}
