package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableGet
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem }
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

//noinspection TypeAnnotation
object BatchingModelSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Model")(batchGetItemSuite, batchWriteItemSuite)

  val batchGetItemSuite = suite("BatchGetItem")(
    test("should aggregate GetItems using +") {
      val batch = BatchGetItem().addAll(getItem1, getItem2)

      assert(batch.addList)(equalTo(Chunk(getItem1, getItem2))) &&
      assert(batch.requestItems)(
        equalTo(
          MapOfSet.empty.addAll(
            tableName1 -> TableGet(getItem1.key, getItem1.projections),
            tableName1 -> TableGet(getItem2.key, getItem2.projections)
          )
        )
      )
    },
    test("with aggregated GetItem's should return Some values back when keys are found") {
      val batch    = BatchGetItem().addAll(getItem1, getItem2)
      val response =
        BatchGetItem.Response(MapOfSet.empty.addAll(tableName1 -> item("k1"), tableName1 -> item("k2")))

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(Some(item("k1")), Some(item("k2")))))
    },
    test("with aggregated GetItem's should return None back for keys that are not found") {
      val batch    = BatchGetItem().addAll(getItem1, getItem2)
      val response =
        BatchGetItem.Response(
          MapOfSet.empty.addAll(tableName1 -> item("NotAKey1"), tableName1 -> item("k2"))
        )

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(None, Some(item("k2")))))
    }
  )

  val batchWriteItemSuite = suite("BatchWriteItem")(
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
