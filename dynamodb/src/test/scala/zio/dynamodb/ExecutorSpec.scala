package zio.dynamodb

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.fake.FakeDynamoDBExecutor
import zio.test.Assertion._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object ExecutorSpec extends DefaultRunnableSpec with DynamoDBFixtures {

  private val executorWithOneTable = FakeDynamoDBExecutor
    .table(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
    .layer

  private val executorWithTwoTables = FakeDynamoDBExecutor
    .table(tableName1.value, "k1")(primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
    .table(tableName3.value, "k3")(primaryKeyT3 -> itemT3)
    .layer

  override def spec: ZSpec[Environment, Failure] = suite("Batching")(crudSuite, scanAndQuerySuite, batchingSuite)
  private val crudSuite                          = suite("single Item CRUD suite")(
    testM("getItem") {
      for {
        result  <- getItemT1.execute
        expected = Some(itemT1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithTwoTables),
    testM("getItem returns an error when table does not exist") {
      for {
        result <- getItem("TABLE_DOES_NOT_EXISTS", primaryKeyT1).execute.either
      } yield assert(result)(isLeft)
    }.provideLayer(executorWithOneTable),
    testM("should execute putItem then getItem when sequenced in a ZIO") {
      for {
        _       <- putItemT1.execute
        result  <- getItemT1.execute
        expected = Some(itemT1)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(FakeDynamoDBExecutor.table(tableName1.value, "k1")().layer),
    testM("putItem returns an error when table does not exist") {
      for {
        result <- putItem("TABLE_DOES_NOT_EXISTS", itemT1).execute.either
      } yield assert(result)(isLeft)
    }.provideLayer(executorWithOneTable),
    testM("should delete an item") {
      for {
        result1 <- getItemT1.execute
        _       <- deleteItem(tableName1.value, primaryKeyT1).execute
        result2 <- getItemT1.execute
        expected = Some(itemT1)
      } yield assert(result1)(equalTo(expected)) && assert(result2)(equalTo(None))
    }.provideLayer(executorWithTwoTables),
    testM("deleteItem returns an error when table does not exist") {
      for {
        result <- deleteItem("TABLE_DOES_NOT_EXISTS", primaryKeyT1).execute.either
      } yield assert(result)(isLeft)
    }.provideLayer(executorWithOneTable)
  )
  private val scanAndQuerySuite                  = suite("Scan and Query suite")(
    testM(
      "scanSome returns an error when table does not exist"
    ) {
      for {
        errorOrResult <- scanSome("TABLE_DOES_NOT_EXISTS", "k1", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    testM("scanSome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        t           <- scanSome(tableName1.value, "k1", 10).execute
        (chunk, lek) = t
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    },
    testM(
      "querySome returns an error when table does not exist"
    ) {
      for {
        errorOrResult <- querySome("TABLE_DOES_NOT_EXISTS", "k1", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    testM(
      "querySome with limit less than table size should return partial result chunk and return a LEK of last read Item"
    ) {
      for {
        t           <- querySome(tableName1.value, "k1", 3).execute
        (chunk, lek) = t
      } yield {
        val item3 = Item("k1" -> 3)
        assert(chunk)(equalTo(resultItems(1 to 3))) && assert(lek)(equalTo(Some(item3)))
      }
    },
    testM("querySome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        t           <- querySome(tableName1.value, "k1", 10).execute
        (chunk, lek) = t
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
  ).provideLayer(
    FakeDynamoDBExecutor.table(tableName1.value, "k1")(chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*).layer
  )
  private val batchingSuite                      = suite("batching should")(
    testM("batch putItem1 zip putItem1_2") {
      for {
        result      <- (putItemT1 zip putItemT1_2).execute
        table1Items <- (getItemT1 zip getItemT1_2).execute
      } yield assert(result)(equalTo(())) && assert(table1Items)(equalTo((Some(itemT1), Some(itemT1_2))))
    }.provideLayer(FakeDynamoDBExecutor.table(tableName1.value, "k1")().layer),
    testM("batch getItem1 zip getItem2 zip getItem3 returns 3 items that are found") {
      for {
        result  <- (getItemT1 zip getItemT1_2 zip getItemT3).execute
        expected = (Some(itemT1), Some(itemT1_2), Some(itemT3))
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithTwoTables),
    testM("batch getItem1 zip getItem2 zip getItem3 returns 2 items that are found") {
      for {
        result  <- (getItemT1 zip getItemT1_2 zip getItemT1_NotExists).execute
        expected = (Some(itemT1), Some(itemT1_2), None)
      } yield assert(result)(equalTo(expected))
    }.provideLayer(executorWithOneTable),
    testM("batch putItem1 zip getItem1 zip getItem2 zip deleteItem1") {
      for {
        result      <- (putItemT3_2 zip getItemT1 zip getItemT1_2 zip deleteItemT3).execute
        expected     = (Some(itemT1), Some(itemT1_2))
        table3Items <- (getItemT3 zip getItemT3_2).execute
      } yield assert(result)(equalTo(expected)) && assert(table3Items)(equalTo((None, Some(itemT3_2))))
    }.provideLayer(executorWithTwoTables),
    testM("should execute forEach of GetItems (resulting in a batched request)") {
      for {
        result <- forEach(1 to 2) { i =>
                    getItem(tableName1.value, PrimaryKey("k1" -> s"v$i"))
                  }.execute
      } yield assert(result)(equalTo(List(Some(itemT1), Some(itemT1_2))))
    }.provideLayer(executorWithTwoTables)
  )

}
