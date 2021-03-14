package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem }
import zio.dynamodb.TestFixtures._
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

import scala.collection.immutable.{ Map => ScalaMap }
object BatchingModelSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Model")(batchGetItemSuite, batchWriteItemSuite)

  val batchGetItemSuite = suite("BatchGetItem")(
    test("should aggregate GetItems using +") {
      val batch = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2

      assert(batch.addList)(equalTo(Chunk(getItem1, getItem2)))
    },
    test("with aggregate GetItems should return Some values back when keys are found") {
      val batch    = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2
      val response =
        BatchGetItem.Response(MapOfSet(ScalaMap(tableName1 -> Set(item("k1")), tableName2 -> Set(item("k2")))))

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(Some(item("k1")), Some(item("k2")))))
    },
    test("with aggregate GetItems should return None back for keys that are not found") {
      val batch    = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2
      val response =
        BatchGetItem.Response(
          MapOfSet(ScalaMap(tableName1 -> Set(item("NotAKey1")), tableName2 -> Set(item("NotAKey2"))))
        )

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(None, None)))
    }
  )

  val batchWriteItemSuite = suite("BatchWriteItem")(
    test("should aggregate PutItem's using +") {
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
