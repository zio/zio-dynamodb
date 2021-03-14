package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.BatchGetItem
import zio.dynamodb.TestFixtures._
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }
import scala.collection.immutable.{ Map => ScalaMap }
object BatchingModelSpec extends DefaultRunnableSpec {

  override def spec = suite("Batch Model")(batchModelSuite)

  val batchModelSuite = suite("batching model")(
    test("BatchGetItem should aggregate GetItems using +") {
      val batch = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2

      assert(batch.addList)(equalTo(Chunk(getItem1, getItem2)))
    },
    test("a BatchGetItem with aggregate GetItems should return Some values back when keys are found") {
      val batch    = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2
      val response =
        BatchGetItem.Response(MapOfSet(ScalaMap(tableName1 -> Set(item("k1")), tableName2 -> Set(item("k2")))))

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(Some(item("k1")), Some(item("k2")))))
    },
    test("a BatchGetItem with aggregate GetItems should return None back for keys that are not found") {
      val batch    = (BatchGetItem(MapOfSet.empty) + getItem1) + getItem2
      val response =
        BatchGetItem.Response(
          MapOfSet(ScalaMap(tableName1 -> Set(item("NotAKey1")), tableName2 -> Set(item("NotAKey2"))))
        )

      assert(batch.toGetItemResponses(response))(equalTo(Chunk(None, None)))
    }
  )

}
