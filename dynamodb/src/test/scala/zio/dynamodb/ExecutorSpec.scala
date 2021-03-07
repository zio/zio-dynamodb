package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, Map, PutItem, Scan }
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.{ assert, DefaultRunnableSpec }

import scala.collection.immutable.{ Map => ScalaMap }

object ExecutorSpec extends DefaultRunnableSpec {

  val emptyItem                                               = Item(ScalaMap.empty)
  val primaryKey                                              = PrimaryKey(ScalaMap.empty)
  val tableName1                                              = TableName("T1")
  val tableName2                                              = TableName("T2")
  val indexName1                                              = IndexName("I1")
  val getItem1                                                = GetItem(key = primaryKey, tableName = tableName1)
  val getItem2                                                = GetItem(key = primaryKey, tableName = tableName2)
  val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2
  val map1                                                    = Map(
    getItem1,
    (o: Option[Item]) => o.map(_ => Item(ScalaMap("1" -> AttributeValue.String("2"))))
  )

  val putItem1                                  = PutItem(tableName = tableName1, item = Item(ScalaMap.empty))
  val deleteItem1                               = DeleteItem(tableName = tableName2, key = PrimaryKey(ScalaMap.empty))
  val zippedWrites: DynamoDBQuery[(Unit, Unit)] = putItem1 zip deleteItem1
  val stream1                                   = ZStream(emptyItem)
  val scan1                                     = Scan(tableName1, indexName1)

  def item(a: String): Item             = Item(ScalaMap(a -> AttributeValue.String(a)))
  def someItem(a: String): Option[Item] = Some(item(a))

  override def spec =
    suite(label = "Executor.parallelize")(
      test(label = "should aggregate Zipped GetItems") {
        val tuple       = DynamoDBExecutor.parallelize(zippedGets)
        val constructor = tuple._1
        val assembled   = tuple._2(Chunk(someItem("1"), someItem("2")))
        val expected    = (someItem("1"), someItem("2"))

        assert(constructor)(equalTo(Chunk(getItem1, getItem2))) &&
        assert(assembled)(equalTo(expected))

      },
      test("should aggregate Zipped writes") {
        val tuple       = DynamoDBExecutor.parallelize(zippedWrites)
        val constructor = tuple._1
        val assembled   = tuple._2(Chunk((), ()))

        assert(constructor)(equalTo(Chunk(putItem1, deleteItem1))) &&
        assert(assembled)(equalTo(((), ())))
      },
      test("should process Map constructor") {
        val tuple       = DynamoDBExecutor.parallelize(map1)
        val constructor = tuple._1
        val assembled   = tuple._2(Chunk(someItem("1")))

        assert(constructor)(equalTo(Chunk(getItem1))) && assert(assembled)(
          equalTo(Some(Item(ScalaMap("1" -> AttributeValue.String("2")))))
        )
      },
      test("should process Scan constructor") {
        val tuple       = DynamoDBExecutor.parallelize(scan1)
        val constructor = tuple._1
        val assembled   = tuple._2(Chunk((stream1, None)))

        assert(constructor)(equalTo(Chunk(scan1))) &&
        assert(assembled)(equalTo((stream1, None)))
      }
    ).provideCustomLayer(DynamoDb.test(item("1")))
}
