package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, GetItem }
import zio.test.Assertion._
import zio.test._
import zio.test.ZIOSpecDefault

object BatchingModelSpec extends ZIOSpecDefault with DynamoDBFixtures {

  override def spec: Spec[Environment, Any] = suite("Batch Model")(batchGetItemSuite, batchWriteItemSuite)

  private val batchGetItemSuite = suite("BatchGetItem")(
    test("should aggregate GetItems using +") {
      val itemT1: GetItem   = GetItem(tableName1, primaryKeyT1, List(ProjectionExpression.Root("field1")))
      val itemT1_2: GetItem = GetItem(
        tableName1,
        primaryKeyT2,
        List(ProjectionExpression.Root("field2"))
      )
      val batch             = BatchGetItem().addAll(itemT1, itemT1_2)

      assert(batch.orderedGetItems)(equalTo(Chunk(itemT1, itemT1_2))) &&
      assert(batch.requestItems)(
        equalTo(
          Map(
            tableName1 -> TableGet(
              Set(itemT1.key, itemT1_2.key),
              Set(ProjectionExpression.Root("field1"), ProjectionExpression.Root("field2"))
            )
          )
        )
      )
    },
    test("with aggregated GetItem's should return Some values back when keys are found") {
      val batch    = BatchGetItem().addAll(getItemT1, getItemT1_2)
      val response =
        BatchGetItem.Response(MapOfSet.empty.addAll(tableName1 -> itemT1, tableName1 -> itemT1_2))

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(Some(itemT1), Some(itemT1_2))))
    },
    test("with aggregated GetItem's should return None back for keys that are not found") {
      val batch    = BatchGetItem().addAll(getItemT1, getItemT1_2)
      val response =
        BatchGetItem.Response(
          MapOfSet.empty.addAll(tableName1 -> itemT1_NotExists, tableName1 -> itemT1_2)
        )

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(None, Some(itemT1_2))))
    }
  )

  private val batchWriteItemSuite = suite("BatchWriteItem")(
    test("should aggregate a PutItem and then a DeleteItem for the same table using +") {
      val batch: BatchWriteItem = BatchWriteItem().addAll(putItemT1, deleteItemT1)

      assert(batch.addList)(
        equalTo(Chunk(BatchWriteItem.Put(putItemT1.item), BatchWriteItem.Delete(deleteItemT1.key)))
      ) &&
      assert(batch.requestItems)(
        equalTo(
          MapOfSet
            .empty[TableName, BatchWriteItem.Write]
            .addAll(
              tableName1 -> BatchWriteItem.Put(putItemT1.item),
              tableName1 -> BatchWriteItem.Delete(deleteItemT1.key)
            )
        )
      )
    }
  )

}
