package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem }
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

object BatchingModelSpec extends DefaultRunnableSpec with DynamoDBFixtures {

  override def spec: ZSpec[Environment, Failure] = suite("Batch Model")(batchGetItemSuite, batchWriteItemSuite)

  private val batchGetItemSuite = suite("BatchGetItem")(
    test("should aggregate GetItems using +") {
      val batch = BatchGetItem().addAll(getItemT1, getItemT2)

      assert(batch.addList)(equalTo(Chunk(getItemT1, getItemT2))) &&
      assert(batch.requestItems)(
        equalTo(
          MapOfSet.empty.addAll(
            tableName1 -> TableGet(getItemT1.key, getItemT1.projections),
            tableName1 -> TableGet(getItemT2.key, getItemT2.projections)
          )
        )
      )
    },
    test("with aggregated GetItem's should return Some values back when keys are found") {
      val batch    = BatchGetItem().addAll(getItemT1, getItemT2)
      val response =
        BatchGetItem.Response(MapOfSet.empty.addAll(tableName1 -> itemT1, tableName1 -> itemT2))

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(Some(itemT1), Some(itemT2))))
    },
    test("with aggregated GetItem's should return None back for keys that are not found") {
      val batch    = BatchGetItem().addAll(getItemT1, getItemT2)
      val response =
        BatchGetItem.Response(
          MapOfSet.empty.addAll(tableName1 -> item("NotAKey1"), tableName1 -> itemT2)
        )

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(None, Some(itemT2))))
    }
  )

  private val batchWriteItemSuite = suite("BatchWriteItem")(
    test("should aggregate a PutItem and then a DeleteItem for the same table using +") {
      val batch: BatchWriteItem = BatchWriteItem().addAll(putItem1, deleteItem1)

      assert(batch.addList)(
        equalTo(Chunk(BatchWriteItem.Put(putItem1.item), BatchWriteItem.Delete(deleteItem1.key)))
      ) &&
      assert(batch.requestItems)(
        equalTo(
          MapOfSet
            .empty[TableName, BatchWriteItem.Write]
            .addAll(
              tableName1 -> BatchWriteItem.Put(putItem1.item),
              tableName1 -> BatchWriteItem.Delete(deleteItem1.key)
            )
        )
      )
    }
  )

}
