package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery._
import zio.test.Assertion._
import zio.test.{ assert, TestAspect, ZIOSpecDefault }
import zio.test.Spec

object BatchingDSLSpec extends ZIOSpecDefault with DynamoDBFixtures {

  private val beforeAddTable1          = TestAspect.before(
    TestDynamoDBExecutor
      .addTable(tableName1.value, "k1", primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
  )
  private val beforeAddTable1AndTable2 = TestAspect.before(
    TestDynamoDBExecutor
      .addTable(tableName1.value, "k1", primaryKeyT1                     -> itemT1, primaryKeyT1_2 -> itemT1_2) *>
      TestDynamoDBExecutor.addTable(tableName3.value, "k3", primaryKeyT3 -> itemT3)
  )

  override def spec: Spec[Environment, Any] =
    suite("Batching")(crudSuite, scanAndQuerySuite, batchingSuite).provideLayer(DynamoDBExecutor.test)

  private val crudSuite = suite("single Item CRUD suite")(
    test("getItemX") {
      for {
        _      <- TestDynamoDBExecutor.addTable(tableName1.value, "k1", primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
        result <- getItemT1.execute
      } yield assert(result)(equalTo(Some(itemT1)))
    },
    test("getItem returns an error when table does not exist") {
      for {
        result <- getItem("TABLE_DOES_NOT_EXISTS", primaryKeyT1).execute.either
      } yield assert(result)(isLeft)
    },
    test("should execute putItem then getItem when sequenced in a ZIO") {
      for {
        _      <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")
        _      <- putItemT1.execute
        result <- getItemT1.execute
      } yield assert(result)(equalTo(Some(itemT1)))
    },
    test("putItem returns an error when table does not exist") {
      for {
        result <- putItem("TABLE_DOES_NOT_EXISTS", itemT1).execute.either
      } yield assert(result)(isLeft)
    },
    test("should delete an item") {
      for {
        _            <- TestDynamoDBExecutor.addTable(tableName1.value, "k1", primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
        beforeDelete <- getItemT1.execute
        _            <- deleteItem(tableName1.value, primaryKeyT1).execute
        afterDelete  <- getItemT1.execute
      } yield assert(beforeDelete)(equalTo(Some(itemT1))) && assert(afterDelete)(equalTo(None))
    },
    test("deleteItem returns an error when table does not exist") {
      for {
        result <- deleteItem("TABLE_DOES_NOT_EXISTS", primaryKeyT1).execute.either
      } yield assert(result)(isLeft)
    }
  )

  private val scanAndQuerySuite = suite("Scan and Query suite")(
    test(
      "scanSome returns an error when table does not exist"
    ) {
      for {
        errorOrResult <- scanSomeItem("TABLE_DOES_NOT_EXISTS", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    test("scanSome on an empty table returns empty results and a LEK of None") {
      for {
        r <- scanSomeItem(tableName2.value, 10).execute
      } yield assert(r._1)(equalTo(Chunk.empty)) && assert(r._2)(equalTo(None))
    },
    test("scanSome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        r <- scanSomeItem(tableName1.value, 10).execute
      } yield assert(r._1)(equalTo(resultItems(1 to 5))) && assert(r._2)(equalTo(None))
    },
    test(
      "querySome returns an error when table does not exist"
    ) {
      for {
        errorOrResult <- querySomeItem("TABLE_DOES_NOT_EXISTS", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    test("querySome on an empty table returns empty results and a LEK of None") {
      for {
        r <- querySomeItem(tableName2.value, 10).execute
      } yield assert(r._1)(equalTo(Chunk.empty)) && assert(r._2)(equalTo(None))
    },
    test(
      "querySome with limit less than table size should return partial result chunk and return a LEK of last read Item"
    ) {
      for {
        r <- querySomeItem(tableName1.value, 3).execute
      } yield {
        val item3 = Item("k1" -> 3)
        assert(r._1)(equalTo(resultItems(1 to 3))) && assert(r._2)(equalTo(Some(item3)))
      }
    },
    test("querySome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        r <- querySomeItem(tableName1.value, 10).execute
      } yield assert(r._1)(equalTo(resultItems(1 to 5))) && assert(r._2)(equalTo(None))
    },
    test("scanAll should scan all items in a table") {
      for {
        stream <- scanAllItem(tableName1.value).execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    },
    test("queryAll should scan all items in a table") {
      for {
        stream <- queryAllItem(tableName1.value).execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    }
  ) @@ TestAspect.before(
    TestDynamoDBExecutor.addTable(
      tableName1.value,
      "k1",
      chunkOfPrimaryKeyAndItem(1 to 5, "k1"): _*
    ) *> TestDynamoDBExecutor.addTable(tableName2.value, "k2")
  )

  private val batchingSuite = suite("batching should")(
    test("batch putItem1 zip putItem1_2") {
      for {
        _                                                             <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")
        query1: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap])] = putItemT1 zip putItemT1_2
        query2: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap])] = getItemT1 zip getItemT1_2
        putItemsResults                                               <- query1.execute
        getItemsResults                                               <- query2.execute
      } yield assert(putItemsResults)(equalTo((None, None))) && assert(getItemsResults)(
        equalTo((Some(itemT1), Some(itemT1_2)))
      )
    },
    test("batch getItem1 zip getItem2 zip getItem3 returns 3 items that are found") {
      val query1: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap])]                  = getItemT1 zip getItemT1_2
      val query2: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap], Option[AttrMap])] = query1 zip getItemT3
      for {
        result  <- query2.execute
        expected = (Some(itemT1), Some(itemT1_2), Some(itemT3))
      } yield assert(result)(equalTo(expected))
    } @@ beforeAddTable1AndTable2,
    test("batch getItem1 zip getItem2 zip getItem3 returns 2 items that are found") {
      val query1: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap])]                  = getItemT1 zip getItemT1_2
      val query2: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap], Option[AttrMap])] =
        query1 zip getItemT1_NotExists
      for {
        result  <- query2.execute
        expected = (Some(itemT1), Some(itemT1_2), None)
      } yield assert(result)(equalTo(expected))
    } @@ beforeAddTable1,
    test("batch putItem1 zip getItem1 zip getItem2 zip deleteItem1") {
      val query1: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap])]                                   = putItemT3_2 zip getItemT1
      val query2: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap], Option[AttrMap])]                  = query1 zip getItemT1_2
      val query3: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap], Option[AttrMap], Option[AttrMap])] =
        query2 zip deleteItemT3

      val nextQuery: DynamoDBQuery[Any, (Option[AttrMap], Option[AttrMap])] = getItemT3 zip getItemT3_2
      for {
        mixedOpsResults <- query3.execute
        mixedOpsExpected = (None, Some(itemT1), Some(itemT1_2), None)
        getsResults     <- nextQuery.execute
      } yield assert(mixedOpsResults)(equalTo(mixedOpsExpected)) && assert(getsResults)(equalTo((None, Some(itemT3_2))))
    } @@ beforeAddTable1AndTable2,
    test("should execute forEach of GetItems (resulting in a batched request)") {
      for {
        result <- forEach(1 to 2) { i =>
                    getItem(tableName1.value, PrimaryKey("k1" -> s"v$i"))
                  }.execute
      } yield assert(result)(equalTo(List(Some(itemT1), Some(itemT1_2))))
    } @@ beforeAddTable1AndTable2
  )
}
