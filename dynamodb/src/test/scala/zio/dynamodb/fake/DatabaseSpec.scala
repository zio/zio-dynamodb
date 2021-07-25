package zio.dynamodb.fake

import zio.Chunk
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.fake.Database.{ resultItems, tableEntries }
import zio.dynamodb.{ BatchingFixtures, Item, PrimaryKey }
import zio.test.Assertion._
import zio.test._

object DatabaseSpec extends DefaultRunnableSpec with BatchingFixtures {

  private val noLastEvaluatedKey = None

  override def spec: ZSpec[Environment, Failure] =
    suite("FakeDynamoDB")(fakeDatabaseSuite)

  private val dbWithEmptyTable = Database().table(tableName1.value, "k1")()

  private val dbWithFiveItems = Database()
    .table(tableName1.value, "k1")(tableEntries(1 to 5, "k1"): _*)

  private val fakeDatabaseSuite = suite("FakeDatabase suite")(
    test("getItem returns an error when table does not exists") {
      val db = Database()
      assert(db.getItem("T1", primaryKey1))(isLeft)
    },
    test("getItem returns Some item when created using table()") {
      val db = Database().table("T1", "k1")(primaryKey1 -> item1)
      assert(db.getItem("T1", primaryKey1))(equalTo(Right(Some(item1))))
    },
    test("getItem returns None when primary key does not exist") {
      val db = Database().table("T1", "k1")(primaryKey1 -> item1)
      assert(db.getItem("T1", primaryKey2))(equalTo(Right(None)))
    },
    test("getItem returns Some item from correct table when there are multiple tables") {
      val db = Database()
        .table("T1", "k1")(primaryKey1 -> item1)
        .table("t2", "k2")(primaryKey2 -> item2)
      assert(db.getItem("T1", primaryKey1))(equalTo(Right(Some(item1)))) && assert(db.getItem("t2", primaryKey2))(
        equalTo(Right(Some(item2)))
      )
    },
    test("put() returns an error when table does not exists") {
      val db              = Database()
      val errorOrDatabase = db.put("T1", item1)
      assert(errorOrDatabase)(isLeft)
    },
    test("put() updates a table created using table()") {
      val db              = Database().table("T1", "k1")()
      val errorOrDatabase = db.put("T1", item1)
      assert(db.getItem("T1", primaryKey1))(isRight(equalTo(None))) && assert(
        errorOrDatabase.flatMap(_.getItem("T1", primaryKey1))
      )(
        equalTo(Right(Some(item1)))
      )
    },
    test("delete() returns a Left of error when table does not exists") {
      val db        = Database()
      val errorOrDb = db.delete("T1", primaryKey1)
      assert(errorOrDb)(isLeft)
    },
    test("delete() removes an entry created using table()") {
      val db        = Database().table("T1", "k1")(primaryKey1 -> item1)
      val errorOrDb = db.delete("T1", primaryKey1)
      assert(db.getItem("T1", primaryKey1))(equalTo(Right(Some(item1)))) && assert(
        errorOrDb.flatMap(_.getItem("T1", primaryKey1))
      )(
        equalTo(Right(None))
      )
    },
    test("""scanSome with a table that does not exists results in an error""") {
      val db            = dbWithEmptyTable
      val errorOrResult = db.scanSome("TABLE_DOES_NOT_EXISTS", noLastEvaluatedKey, 2)
      assert(errorOrResult)(isLeft)
    },
    test("""scanSome("T1", None, 2) on an empty table""") {
      val db            = dbWithEmptyTable
      val errorOrResult = db.scanSome("T1", noLastEvaluatedKey, 2)
      assert(errorOrResult)(isRight(equalTo((Chunk.empty, None))))
    },
    test("""scanSome("T1", Some(PrimaryKey("k1" -> 1)), 2) on an empty table""") {
      val db            = dbWithEmptyTable
      val errorOrResult = db.scanSome("T1", Some(PrimaryKey("k1" -> 1)), 2)
      assert(errorOrResult)(isRight(equalTo((Chunk.empty, None))))
    },
    test("""scanSome("T1", None, 10) on 5 Items""") {
      val db            = dbWithFiveItems
      val errorOrResult = db.scanSome("T1", noLastEvaluatedKey, 10)
      assert(errorOrResult)(
        isRight(equalTo((resultItems(1 to 5), None)))
      )
    },
    test("""scanSome("T1", None, 2) on 5 Items""") {
      val db            = dbWithFiveItems
      val errorOrResult = db.scanSome("T1", noLastEvaluatedKey, 2)
      assert(errorOrResult)(
        isRight(equalTo((resultItems(1 to 2), lastEvaluatedKey(2))))
      )
    },
    test("""scanSome("T1", Some(PrimaryKey("k1" -> 1)), 2) on 5 Items""") {
      val db            = dbWithFiveItems
      val errorOrResult = db.scanSome("T1", Some(PrimaryKey("k1" -> 1)), 2)
      assert(errorOrResult)(
        isRight(equalTo((resultItems(2 to 3), lastEvaluatedKey(3))))
      )
    },
    test("""scanSome("T1", Some(PrimaryKey("k1" -> 3)), 2) on 5 Items""") {
      val db            = dbWithFiveItems
      val errorOrResult = db.scanSome("T1", Some(PrimaryKey("k1" -> 3)), 2)
      assert(errorOrResult)(
        isRight(equalTo((resultItems(4 to 5), None)))
      )
    },
    test("""scanSome("T1", Some(PrimaryKey("k1" -> 4)), 2) on 5 Items""") {
      val db            = dbWithFiveItems
      val errorOrResult = db.scanSome("T1", Some(PrimaryKey("k1" -> 4)), 2)
      assert(errorOrResult)(
        isRight(equalTo((resultItems(5 to 5), None)))
      )
    },
    testM("""scanAll("T1") on 5 Items""") {
      val db = dbWithFiveItems
      for {
        chunk <- db.scanAll("T1").runCollect.orDie
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    }
  )

  private def lastEvaluatedKey(value: Int): Option[Item] = Some(PrimaryKey("k1" -> value))

}
