package zio.dynamodb

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery.DeleteItem
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object FakeDynamoDBSpec extends DefaultRunnableSpec with BatchingFixtures {

  override def spec: ZSpec[Environment, Failure] =
    suite("FakeDynamoDB")(fakeDynamoDbSuite, fakeDatabaseSuite)

  private val dbWithTwoTables   = Database()
    .table(tableName1.value, "k1")(primaryKey1 -> item1, primaryKey1_2 -> item1_2)
    .table(tableName3.value, "k3")(primaryKey3 -> item3)

  private val fakeDatabaseSuite = suite("FakeDatabase suite")(
    test("getItem returns Some item when created using table()") {
      val db = Database().table("T1", "k1")(primaryKey1 -> item1)
      assert(db.getItem("T1", primaryKey1))(equalTo(Some(item1)))
    },
    test("getItem returns None when primary key does not exist") {
      val db = Database().table("T1", "k1")(primaryKey1 -> item1)
      assert(db.getItem("T1", primaryKey2))(equalTo(None))
    },
    test("getItem returns Some item from correct table when there are multiple tables") {
      val db = Database()
        .table("T1", "k1")(primaryKey1 -> item1)
        .table("t2", "k2")(primaryKey2 -> item2)
      assert(db.getItem("T1", primaryKey1))(equalTo(Some(item1))) && assert(db.getItem("t2", primaryKey2))(
        equalTo(Some(item2))
      )
    },
    test("put() updates a table created using table()") {
      val db            = Database().table("T1", "k1")()
      val maybeDatabase = db.put("T1", item1)
      assert(db.getItem("T1", primaryKey1))(equalTo(None)) && assert(
        maybeDatabase.map(_.getItem("T1", primaryKey1)).flatten
      )(
        equalTo(Some(item1))
      )
    },
    test("remove() removes an entry created using table()") {
      val db            = Database().table("T1", "k1")(primaryKey1 -> item1)
      val maybeDatabase = db.remove("T1", primaryKey1)
      assert(db.getItem("T1", primaryKey1))(equalTo(Some(item1))) && assert(
        maybeDatabase.map(_.getItem("T1", primaryKey1)).flatten
      )(
        equalTo(None)
      )
    }
  )

  private val fakeDynamoDbSuite = suite("FakeDynamoDB suite")(
    testM("getItem") {
      for {
        result  <- getItem1.execute
        expected = Some(item1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(FakeDynamoDBExecutor(dbWithTwoTables)),
    testM("should execute putItem then getItem when sequenced in a ZIO") {
      for {
        _       <- putItem1.execute
        result  <- getItem1.execute
        expected = Some(item1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(
      FakeDynamoDBExecutor(Database().table(tableName1.value, "k1")())
    ),
    testM("should execute getItem1 zip getItem2 zip getItem3") {
      for {
        assembled <- (getItem1 zip getItem1_2 zip getItem3).execute
      } yield assert(assembled)(equalTo((Some(item1), Some(item1_2), Some(item3))))
    }.provideLayer(FakeDynamoDBExecutor(dbWithTwoTables)),
    testM("should remove an item") {
      for {
        result1 <- getItem1.execute
        _       <- DeleteItem(tableName1, PrimaryKey("k1" -> "k1")).execute
        result2 <- getItem1.execute
        expected = Some(item1)
      } yield assert(result1)(equalTo(expected)) && assert(result2)(equalTo(None))
    }.provideLayer(
      FakeDynamoDBExecutor(dbWithTwoTables)
    )
  )

}
