package zio.dynamodb

import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.DynamoDb.DynamoDbMock
import io.github.vigoo.zioaws.dynamodb.model.{
  AttributeValue => ZIOAwsAttributeValue,
  BatchGetItemResponse => ZIOAwsBatchGetItemResponse,
  BatchWriteItemResponse,
  KeysAndAttributes => ZIOAwsKeysAndAttributes
}
import zio.clock.Clock
import zio.{ Chunk, ULayer, ZLayer }
import zio.dynamodb.DynamoDBQuery._

import scala.collection.immutable.{ Map => ScalaMap }
import zio.duration._
import zio.test.Assertion._
import zio.test.environment.{ testEnvironment, Live, TestClock }
import zio.test.mock.Expectation.value
import zio.test.{ assert, Annotations, DefaultRunnableSpec, TestAspect, ZSpec }

object ExecutorSpec extends DefaultRunnableSpec with DynamoDBFixtures {

  private val beforeAddTable1          = TestAspect.before(
    TestDynamoDBExecutor
      .addTable(tableName1.value, "k1", primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
  )
  private val beforeAddTable1AndTable2 = TestAspect.before(
    TestDynamoDBExecutor
      .addTable(tableName1.value, "k1", primaryKeyT1                     -> itemT1, primaryKeyT1_2 -> itemT1_2) *>
      TestDynamoDBExecutor.addTable(tableName3.value, "k3", primaryKeyT3 -> itemT3)
  )

  override def spec: ZSpec[Environment, Failure] =
    suite("Executor spec")(
      suite("Batching")(crudSuite, scanAndQuerySuite, batchingSuite).provideLayer(DynamoDBExecutor.test),
      batchRetries
    )

  private val crudSuite = suite("single Item CRUD suite")(
    testM("getItem") {
      for {
        _      <- TestDynamoDBExecutor.addTable(tableName1.value, "k1", primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
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
        _      <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")
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
        _            <- TestDynamoDBExecutor.addTable(tableName1.value, "k1", primaryKeyT1 -> itemT1, primaryKeyT1_2 -> itemT1_2)
        beforeDelete <- getItemT1.execute
        _            <- deleteItem(tableName1.value, primaryKeyT1).execute
        afterDelete  <- getItemT1.execute
      } yield assert(beforeDelete)(equalTo(Some(itemT1))) && assert(afterDelete)(equalTo(None))
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
        errorOrResult <- scanSomeItem("TABLE_DOES_NOT_EXISTS", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    testM("scanSome on an empty table returns empty results and a LEK of None") {
      for {
        (chunk, lek) <- scanSomeItem(tableName2.value, 10).execute
      } yield assert(chunk)(equalTo(Chunk.empty)) && assert(lek)(equalTo(None))
    },
    testM("scanSome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        (chunk, lek) <- scanSomeItem(tableName1.value, 10).execute
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    },
    testM(
      "querySome returns an error when table does not exist"
    ) {
      for {
        errorOrResult <- querySomeItem("TABLE_DOES_NOT_EXISTS", 3).execute.either
      } yield assert(errorOrResult)(isLeft)
    },
    testM("querySome on an empty table returns empty results and a LEK of None") {
      for {
        (chunk, lek) <- querySomeItem(tableName2.value, 10).execute
      } yield assert(chunk)(equalTo(Chunk.empty)) && assert(lek)(equalTo(None))
    },
    testM(
      "querySome with limit less than table size should return partial result chunk and return a LEK of last read Item"
    ) {
      for {
        (chunk, lek) <- querySomeItem(tableName1.value, 3).execute
      } yield {
        val item3 = Item("k1" -> 3)
        assert(chunk)(equalTo(resultItems(1 to 3))) && assert(lek)(equalTo(Some(item3)))
      }
    },
    testM("querySome with limit greater than table size should scan all items in a table and return a LEK of None") {
      for {
        (chunk, lek) <- querySomeItem(tableName1.value, 10).execute
      } yield assert(chunk)(equalTo(resultItems(1 to 5))) && assert(lek)(equalTo(None))
    },
    testM("scanAll should scan all items in a table") {
      for {
        stream <- scanAllItem(tableName1.value).execute
        chunk  <- stream.runCollect
      } yield assert(chunk)(equalTo(resultItems(1 to 5)))
    },
    testM("queryAll should scan all items in a table") {
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
    testM("batch putItem1 zip putItem1_2") {
      for {
        _           <- TestDynamoDBExecutor.addTable(tableName1.value, "k1")
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

  private val mockBatches     = "mockBatches"
  private val itemOne         = Item("k1" -> "v1")
  private val firstGetRequest =
    DynamoDBExecutorImpl.generateBatchGetItemRequest(
      BatchGetItem(
        ScalaMap(
          TableName(mockBatches) -> BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("k1" -> "v1"), PrimaryKey("k1" -> "v2")),
            projectionExpressionSet = Set.empty
          )
        )
      )
    )

  private val retryGetRequest =
    DynamoDBExecutorImpl.generateBatchGetItemRequest(
      BatchGetItem(
        ScalaMap(
          TableName(mockBatches) -> BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("k1" -> "v2")),
            projectionExpressionSet = Set.empty
          )
        )
      )
    )

  private val batchWriteRequest = BatchWriteItem(
    MapOfSet.empty[TableName, BatchWriteItem.Write] + (
      (
        TableName(mockBatches),
        BatchWriteItem.Put(itemOne)
      )
    ),
    exponentialBackoff = 0.seconds,
    retryAttempts = 1
  )

  private val firstWriteRequest =
    DynamoDBExecutorImpl.generateBatchWriteItem(
      batchWriteRequest
    )

  private val mockedBatchGet: ULayer[DynamoDb] = DynamoDbMock
    .BatchGetItem(
      equalTo(firstGetRequest),
      value(
        ZIOAwsBatchGetItemResponse(
          responses = Some(
            ScalaMap(
              mockBatches -> List(
                ScalaMap("k1" -> ZIOAwsAttributeValue(s = Some("v1")))
              )
            )
          ),
          unprocessedKeys = Some(
            ScalaMap(
              mockBatches -> ZIOAwsKeysAndAttributes(
                keys = List(ScalaMap("k1" -> ZIOAwsAttributeValue(s = Some("v2"))))
              )
            )
          )
        ).asReadOnly
      )
    ) ++ DynamoDbMock
    .BatchGetItem(
      equalTo(retryGetRequest),
      value(
        ZIOAwsBatchGetItemResponse(
          responses = Some(
            ScalaMap(
              mockBatches -> List(
                ScalaMap(
                  "k1" -> ZIOAwsAttributeValue(s = Some("v2")),
                  "k2" -> ZIOAwsAttributeValue(s = Some("v23"))
                )
              )
            )
          ),
          unprocessedKeys = None
        ).asReadOnly
      )
    )

  private val batchGetSuite =
    suite("retry batch gets")(
      testM("should retry when there are unprocessed keys") {
        for {
          response <- BatchGetItem(
                        requestItems = ScalaMap(
                          TableName(mockBatches) -> BatchGetItem.TableGet(
                            Set(PrimaryKey("k1" -> "v1"), PrimaryKey("k1" -> "v2")),
                            Set.empty
                          )
                        ),
                        exponentialBackoff = 0.seconds
                      ).execute
        } yield assert(response.responses.get(TableName(mockBatches)))(
          equalTo(Some(Set(itemOne, Item("k1" -> "v2", "k2" -> "v23"))))
        )
      }
    )

  private val mockedBatchWrite: ULayer[DynamoDb] = DynamoDbMock
    .BatchWriteItem(
      equalTo(firstWriteRequest),
      value(
        BatchWriteItemResponse(
          unprocessedItems = Some(
            ScalaMap(
              mockBatches -> Set(
                DynamoDBExecutorImpl.batchItemWriteToZIOAwsWriteRequest(BatchWriteItem.Put(itemOne))
              )
            )
          )
        ).asReadOnly
      )
    )
    .atMost(2)

  private val batchWriteSuite =
    suite("retry batch writes")(
      testM("should retry when there are unprocessed items") {
        for {
          response <- batchWriteRequest.execute

        } yield assert(response.unprocessedItems)(
          equalTo(
            Some(
              MapOfSet
                .empty[TableName, BatchWriteItem.Write] + (
                (
                  TableName(mockBatches),
                  BatchWriteItem.Put(itemOne)
                )
              )
            )
          )
        )
      }
    )

  private val clockLayer: ZLayer[Any, Nothing, Clock with TestClock] =
    testEnvironment >>> ((Annotations.live ++ Live.default) >>> TestClock.default)

  private val batchRetries = suite("Batch retries")(
    batchGetSuite.provideLayer((mockedBatchGet ++ clockLayer) >>> DynamoDBExecutor.live),
    batchWriteSuite.provideLayer((mockedBatchWrite ++ clockLayer) >>> DynamoDBExecutor.live)
  )
}
