package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.BatchGetItem.TableItem
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem }
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

import scala.collection.immutable.{ Map => ScalaMap }

//noinspection TypeAnnotation
object BatchingModelSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Model")(batchGetItemSuite, batchWriteItemSuite)

  val batchGetItemSuite = suite("BatchGetItem")(
    test("should aggregate GetItems using +") {
      val batch = BatchGetItem(MapOfSet.empty) + getItem1

      assert(batch.addList)(equalTo(Chunk(getItem1))) &&
      assert(batch.requestItems)(
        equalTo(
          MapOfSet[TableName, TableItem](
            ScalaMap(
              tableName1 -> Set(
                TableItem(getItem1.key, getItem1.projections)
              )
            )
          )
        )
      )
    },
    test("with aggregated GetItem's should return Some values back when keys are found") {
      val batch    = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2
      val response =
        BatchGetItem.Response(MapOfSet.empty.addAll(tableName1 -> item("k1"), tableName1 -> item("k2")))

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(Some(item("k1")), Some(item("k2")))))
    },
    test("with aggregated GetItem's should return None back for keys that are not found") {
      val batch    = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2
      val response =
        BatchGetItem.Response(
          MapOfSet.empty.addAll(tableName1 -> item("NotAKey1"), tableName1 -> item("NotAKey2"))
        )

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(None, None)))
    }
  )

  val batchWriteItemSuite = suite("BatchWriteItem")(
    test("should aggregate a PutItem and then a DeleteItem for the same table using +") {
      val batch: BatchWriteItem = (BatchWriteItem(MapOfSet.empty) + putItem1) + deleteItem1

      assert(batch.addList)(
        equalTo(Chunk(BatchWriteItem.Put(putItem1.item), BatchWriteItem.Delete(deleteItem1.key)))
      ) &&
      assert(batch.requestItems)(
        equalTo(
          MapOfSet[TableName, BatchWriteItem.Write](
            ScalaMap(
              tableName1 -> Set(BatchWriteItem.Put(putItem1.item), BatchWriteItem.Delete(deleteItem1.key))
            )
          )
        )
      )
    }
  )

}
