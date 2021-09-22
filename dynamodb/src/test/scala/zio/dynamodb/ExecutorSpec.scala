package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.fake.TestDynamoDBExecutor
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, TestAspect, ZSpec }

object ExecutorSpec extends DefaultRunnableSpec with DynamoDBFixtures {

  private val beforeAddTable1          = TestAspect.before(
    TestDynamoDBExecutor
      .addTable(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
  )
  private val beforeAddTable1AndTable2 = TestAspect.before(
    TestDynamoDBExecutor
      .addTable(tableName1.value, "k1")(primaryKeyT1                     -> itemT1, primaryKeyT1_2 -> itemT1_2) *>
      TestDynamoDBExecutor.addTable(tableName3.value, "k3")(primaryKeyT3 -> itemT3)
  )

  override def spec: ZSpec[Environment, Failure] =
    suite("Batching")(crudSuite, scanAndQuerySuite, batchingSuite).provideLayer(TestDynamoDBExecutor.test)

  private val crudSuite = suite("single Item CRUD suite")(
    testM("getItem") {
      for {
        _      <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
        result <- getItemT1.execute
      } yield assert(result)(equalTo(Some(itemT1)))
    },
    testM("getItem returns an error when table does not exist") {
      for {
        result <- getItem("TABLE_DOES_NOT_EXISTS", primaryKeyT1).execute.either
      } yield assert(result)(isLeft)
    },
    testM("should execute putItem then getItem when sequenced in a ZIO") {
      for {
        _      <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")()
        _      <- putItemT1.execute
        result <- getItemT1.execute
      } yield assert(result)(equalTo(Some(itemT1)))
    },
    testM("putItem returns an error when table does not exist") {
      for {
        result <- putItem("TABLE_DOES_NOT_EXISTS", itemT1).execute.either
      } yield assert(result)(isLeft)
    },
    testM("should delete an item") {
      for {
        _       <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
        _       <- TestDynamoDBExecutor.addTable(tableName3.value, "k3")(primaryKeyT3 -> itemT3)
        result1 <- getItemT1.execute
        _       <- deleteItem(tableName1.value, primaryKeyT1).execute
        result2 <- getItemT1.execute
      } yield assert(result1)(equalTo(Some(itemT1))) && assert(result2)(equalTo(None))
    },
    testM("deleteItem returns an error when table does not exist") {
      for {
        result <- deleteItem("TABLE_DOES_NOT_EXISTS", primaryKeyT1).execute.either
      } yield assert(result)(isLeft)
    }
  )

  private val scanAndQuerySuite = suite("Scan and Query suite")(
    testM(
      "scanSome returns an error when table does not exist"
    ) {
      for {
        errorOrResult <- scanSome("TABLE_DOES_NOT_EXISTS", "k1", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    testM("scanSome on an empty table returns empty results and a LEK of None") {
      for {
        (chunk, lek) <- scanSome(tableName2.value, "k2", 10).execute
      } yield assert(chunk)(equalTo(Chunk.empty)) && assert(lek)(equalTo(None))
    },
    testM("scanSome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        (chunk, lek) <- scanSome(tableName1.value, "k1", 10).execute
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    },
    testM(
      "querySome returns an error when table does not exist"
    ) {
      for {
        errorOrResult <- querySome("TABLE_DOES_NOT_EXISTS", "k1", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    testM("querySome on an empty table returns empty results and a LEK of None") {
      for {
        (chunk, lek) <- querySome(tableName2.value, "k2", 10).execute
      } yield assert(chunk)(equalTo(Chunk.empty)) && assert(lek)(equalTo(None))
    },
    testM(
      "querySome with limit less than table size should return partial result chunk and return a LEK of last read Item"
    ) {
      for {
        (chunk, lek) <- querySome(tableName1.value, "k1", 3).execute
      } yield {
        val item3 = Item("k1" -> 3)
        assert(chunk)(equalTo(resultItems(1 to 3))) && assert(lek)(equalTo(Some(item3)))
      }
    },
    testM("querySome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        (chunk, lek) <- querySome(tableName1.value, "k1", 10).execute
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    },
    testM("scanAll should scan all items in a table") {
      for {
        stream <- scanAll(tableName1.value, "indexNameIgnored").execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    },
    testM("queryAll should scan all items in a table") {
      for {
        stream <- queryAll(tableName1.value, "indexNameIgnored").execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    }
  ) @@ TestAspect.before(
    TestDynamoDBExecutor.addTable(tableName1.value, "k1")(
      chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*
    ) *> TestDynamoDBExecutor.addTable(tableName2.value, "k2")()
  )

  private val batchingSuite = suite("batching should")(
    testM("batch putItem1 zip putItem1_2") {
      for {
        _           <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")()
        result      <- (putItemT1 zip putItemT1_2).execute
        table1Items <- (getItemT1 zip getItemT1_2).execute
      } yield assert(result)(equalTo(())) && assert(table1Items)(equalTo((Some(itemT1), Some(itemT1_2))))
    },
    testM("batch getItem1 zip getItem2 zip getItem3 returns 3 items that are found") {
      for {
        result  <- (getItemT1 zip getItemT1_2 zip getItemT3).execute
        expected = (Some(itemT1), Some(itemT1_2), Some(itemT3))
      } yield assert(result)(equalTo(expected))
    } @@ beforeAddTable1AndTable2,
    testM("batch getItem1 zip getItem2 zip getItem3 returns 2 items that are found") {
      for {
        result  <- (getItemT1 zip getItemT1_2 zip getItemT1_NotExists).execute
        expected = (Some(itemT1), Some(itemT1_2), None)
      } yield assert(result)(equalTo(expected))
    } @@ beforeAddTable1,
    testM("batch putItem1 zip getItem1 zip getItem2 zip deleteItem1") {
      for {
        result      <- (putItemT3_2 zip getItemT1 zip getItemT1_2 zip deleteItemT3).execute
        expected     = (Some(itemT1), Some(itemT1_2))
        table3Items <- (getItemT3 zip getItemT3_2).execute
      } yield assert(result)(equalTo(expected)) && assert(table3Items)(equalTo((None, Some(itemT3_2))))
    } @@ beforeAddTable1AndTable2,
    testM("should execute forEach of GetItems (resulting in a batched request)") {
      for {
        result <- forEach(1 to 2) { i =>
                    getItem(tableName1.value, PrimaryKey("k1" -> s"v$i"))
                  }.execute
      } yield assert(result)(equalTo(List(Some(itemT1), Some(itemT1_2))))
    } @@ beforeAddTable1AndTable2
  )

}
