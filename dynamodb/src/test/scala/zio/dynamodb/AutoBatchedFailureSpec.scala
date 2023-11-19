package zio.dynamodb

import zio.aws.dynamodb.model.primitives.{ AttributeName, StringAttributeValue, TableName => ZIOAwsTableName }
import zio.aws.dynamodb.model.{
  BatchWriteItemResponse,
  AttributeValue => ZIOAwsAttributeValue,
  BatchGetItemResponse => ZIOAwsBatchGetItemResponse,
  KeysAndAttributes => ZIOAwsKeysAndAttributes
}
import zio.aws.dynamodb.{ DynamoDb, DynamoDbMock }
import zio.dynamodb.DynamoDBQuery._
import zio.mock.Expectation.value
import zio.test.Assertion.{ fails, _ }
import zio.test.{ assertZIO, ZIOSpecDefault }
import zio.{ Schedule, ULayer }

import scala.collection.immutable.{ Map => ScalaMap }
import zio.test.TestAspect
import zio.test.Assertion
import zio.Chunk

object AutoBatchedFailureSpec extends ZIOSpecDefault with DynamoDBFixtures {

  override def spec =
    suite("Executor spec")(
      batchRetries
    )

  private val mockBatches             = "mockBatches"
  private val itemOne                 = Item("k1" -> "v1")
  private val itemTwo                 = Item("k1" -> "v2")
  private val getRequestItemOneAndTwo =
    DynamoDBExecutorImpl.awsBatchGetItemRequest(
      BatchGetItem(
        ScalaMap(
          TableName(mockBatches) -> BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("k1" -> "v1"), PrimaryKey("k1" -> "v2")),
            projectionExpressionSet = Set.empty
          )
        )
      )
    )

  val getRequestItemOne =
    DynamoDBExecutorImpl.awsBatchGetItemRequest(
      BatchGetItem(
        ScalaMap(
          TableName(mockBatches) -> BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("k1" -> "v1")),
            projectionExpressionSet = Set.empty
          )
        )
      )
    )

  private val batchWriteRequestItemOne = BatchWriteItem(
    MapOfSet.empty[TableName, BatchWriteItem.Write] + (
      (
        TableName(mockBatches),
        BatchWriteItem.Put(itemOne)
      )
    ),
    retryPolicy = Schedule.recurs(1)
  )

  private val batchWriteRequestItemOneAndTwo = BatchWriteItem(
    (MapOfSet.empty[TableName, BatchWriteItem.Write] + (
      (
        TableName(mockBatches),
        BatchWriteItem.Put(itemOne)
      )
    ) + (
      TableName(mockBatches),
      BatchWriteItem.Put(itemTwo)
    )),
    retryPolicy = Schedule.recurs(1)
  )

  private val writeRequestItemOne       =
    DynamoDBExecutorImpl.awsBatchWriteItemRequest(
      batchWriteRequestItemOne
    )
  private val writeRequestItemOneAndTwo =
    DynamoDBExecutorImpl.awsBatchWriteItemRequest(
      batchWriteRequestItemOneAndTwo
    )

  private val failedMockBatchGet: ULayer[DynamoDb] = DynamoDbMock
    .BatchGetItem(
      equalTo(getRequestItemOneAndTwo),
      value(
        ZIOAwsBatchGetItemResponse(
          unprocessedKeys = Some(
            ScalaMap(
              ZIOAwsTableName(mockBatches) -> ZIOAwsKeysAndAttributes(
                keys = List(
                  ScalaMap(AttributeName("k1") -> ZIOAwsAttributeValue(s = Some(StringAttributeValue("v2")))),
                  ScalaMap(AttributeName("k1") -> ZIOAwsAttributeValue(s = Some(StringAttributeValue("v1"))))
                )
              )
            )
          )
        ).asReadOnly
      )
    )
    .atMost(4)

  val failedPartialMockBatchGetTwoItems: ULayer[DynamoDb] = DynamoDbMock
    .BatchGetItem(
      equalTo(getRequestItemOneAndTwo),
      value(
        ZIOAwsBatchGetItemResponse(
          unprocessedKeys = Some(
            ScalaMap(
              ZIOAwsTableName(mockBatches) -> ZIOAwsKeysAndAttributes(
                keys = List(
                  ScalaMap(AttributeName("k1") -> ZIOAwsAttributeValue(s = Some(StringAttributeValue("v1"))))
                )
              )
            )
          )
        ).asReadOnly
      )
    )
    .atMost(4)
    .andThen(
      DynamoDbMock
        .BatchGetItem(
          equalTo(getRequestItemOne),
          value(
            ZIOAwsBatchGetItemResponse(
              unprocessedKeys = Some(
                ScalaMap(
                  ZIOAwsTableName(mockBatches) -> ZIOAwsKeysAndAttributes(
                    keys = List(
                      ScalaMap(AttributeName("k1") -> ZIOAwsAttributeValue(s = Some(StringAttributeValue("v1"))))
                    )
                  )
                )
              )
            ).asReadOnly
          )
        )
        .atMost(4)
    )

  private val successfulMockBatchGet: ULayer[DynamoDb] = DynamoDbMock
    .BatchGetItem(
      equalTo(getRequestItemOneAndTwo),
      value(
        ZIOAwsBatchGetItemResponse(
          responses = Some(
            ScalaMap(
              ZIOAwsTableName(mockBatches) -> List(
                ScalaMap(
                  AttributeName("k1") -> ZIOAwsAttributeValue(s = Some(StringAttributeValue("v1")))
                ),
                ScalaMap(
                  AttributeName("k1") -> ZIOAwsAttributeValue(s = Some(StringAttributeValue("v2")))
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
      suite("successful batch gets")(test("should retry when there are unprocessed keys") {
        val autoBatched = getItem("mockBatches", itemOne) zip getItem("mockBatches", itemTwo)
        val programExit = for {
          exit <- autoBatched.execute.exit
        } yield exit
        assertZIO(programExit)(succeeds(anything))
      }).provideLayer(successfulMockBatchGet >>> DynamoDBExecutor.live) @@ TestAspect.withLiveClock,
      suite("all batched requests fail")(
        test("should return all keys in unprocessedKeys for Zipped case") {
          val autoBatched = getItem("mockBatches", itemOne) zip getItem("mockBatches", itemTwo)
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchGetError(
                ScalaMap("mockBatches" -> Set(itemOne, itemTwo))
              )
            )
          )
        },
        test("should return all keys in unprocessedKeys for forEach case") {
          val autoBatched = forEach(List(itemOne, itemTwo))(item => getItem("mockBatches", item))
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchGetError(
                ScalaMap("mockBatches" -> Set(itemOne, itemTwo))
              )
            )
          )
        }
      ).provideLayer(failedMockBatchGet >>> DynamoDBExecutor.live) @@ TestAspect.withLiveClock,
      suite("partial batched requests fail")(
        test("should return failed in unprocessedKeys for Zipped case") {
          val autoBatched = getItem("mockBatches", itemOne) zip getItem("mockBatches", itemTwo)
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchGetError(
                ScalaMap("mockBatches" -> Set(itemOne))
              )
            )
          )
        },
        test("should return failed in unprocessedKeys for forEach case") {
          val autoBatched = forEach(List(itemOne, itemTwo))(item => getItem("mockBatches", item))
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchGetError(
                ScalaMap("mockBatches" -> Set(itemOne))
              )
            )
          )
        }
      ).provideLayer(failedPartialMockBatchGetTwoItems >>> DynamoDBExecutor.live) @@ TestAspect.withLiveClock
    )

  private val itemOneWriteRequest                            = Set(
    DynamoDBExecutorImpl.awsWriteRequest(BatchWriteItem.Put(itemOne))
  )
  private val itemOneAndTwoWriteRequest                      = Set(
    DynamoDBExecutorImpl.awsWriteRequest(BatchWriteItem.Put(itemOne)),
    DynamoDBExecutorImpl.awsWriteRequest(BatchWriteItem.Put(itemTwo))
  )
  private val failedMockBatchWriteTwoItems: ULayer[DynamoDb] = DynamoDbMock
    .BatchWriteItem(
      equalTo(writeRequestItemOneAndTwo),
      value(
        BatchWriteItemResponse(
          unprocessedItems = Some(
            ScalaMap(
              ZIOAwsTableName(mockBatches) -> itemOneAndTwoWriteRequest
            )
          )
        ).asReadOnly
      )
    )
    .atMost(4)

  val failedPartialMockBatchWriteTwoItems: ULayer[DynamoDb] = DynamoDbMock
    .BatchWriteItem(
      equalTo(writeRequestItemOneAndTwo),
      value(
        BatchWriteItemResponse(
          unprocessedItems = Some(
            ScalaMap(
              ZIOAwsTableName(mockBatches) -> itemOneWriteRequest
            )
          )
        ).asReadOnly
      )
    )
    .atMost(4)
    .andThen(
      DynamoDbMock
        .BatchWriteItem(
          equalTo(writeRequestItemOne),
          value(
            BatchWriteItemResponse(
              unprocessedItems = Some(
                ScalaMap(
                  ZIOAwsTableName(mockBatches) -> itemOneWriteRequest
                )
              )
            ).asReadOnly
          )
        )
        .atMost(4)
    )

  private val successfulMockBatchWriteItemOneAndTwo: ULayer[DynamoDb] = DynamoDbMock
    .BatchWriteItem(
      equalTo(writeRequestItemOneAndTwo),
      value(
        BatchWriteItemResponse(
          unprocessedItems = None
        ).asReadOnly
      )
    )

  def assertDynamoDBBatchGetError(map: ScalaMap[String, Set[PrimaryKey]]): Assertion[Any] =
    isSubtype[DynamoDBBatchError.BatchGetError](
      hasField[DynamoDBBatchError.BatchGetError, ScalaMap[String, Set[PrimaryKey]]](
        "unprocessedKeys",
        _.unprocessedKeys,
        equalTo(map)
      )
    )

  private def assertDynamoDBBatchWriteError(map: ScalaMap[String, Chunk[DynamoDBBatchError.Write]]): Assertion[Any] =
    isSubtype[DynamoDBBatchError.BatchWriteError](
      hasField[DynamoDBBatchError.BatchWriteError, ScalaMap[String, Chunk[DynamoDBBatchError.Write]]](
        "unprocessedItems",
        _.unprocessedItems,
        equalTo(map)
      )
    )

  private val batchWriteSuite =
    suite("retry batch writes")(
      suite("successful batch write")(
        test("should return no unprocessedItems for zipped case") {
          val autoBatched = putItem("mockBatches", itemOne) zip putItem("mockBatches", itemTwo)
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(succeeds(anything))
        },
        test("should return no unprocessedItems for forEach case") {
          val autoBatched = forEach(List(itemOne, itemTwo))(item => putItem("mockBatches", item))
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(succeeds(anything))
        }
      ).provideLayer(successfulMockBatchWriteItemOneAndTwo >>> DynamoDBExecutor.live),
      suite("all batched request fail")(
        test("should return all keys in unprocessedItems in Zipped failure case") {
          val autoBatched = putItem("mockBatches", itemOne) zip putItem("mockBatches", itemTwo)
          val programExit = for {
            exit <- autoBatched.execute.exit // TODO: Avi - propogate retry policy GetItem/DeleteItem/PutItem
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchWriteError(
                ScalaMap("mockBatches" -> Chunk(DynamoDBBatchError.Put(itemOne), DynamoDBBatchError.Put(itemTwo)))
              )
            )
          )
        }.provideLayer(failedMockBatchWriteTwoItems >>> DynamoDBExecutor.live) @@ TestAspect.withLiveClock,
        test("should return all keys in unprocessedItems for forEach failure case") {
          val autoBatched = forEach(List(itemOne, itemTwo))(item => putItem("mockBatches", item))
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchWriteError(
                ScalaMap("mockBatches" -> Chunk(DynamoDBBatchError.Put(itemOne), DynamoDBBatchError.Put(itemTwo)))
              )
            )
          )
        }.provideLayer(failedMockBatchWriteTwoItems >>> DynamoDBExecutor.live) @@ TestAspect.withLiveClock
      ),
      suite("partial batch write failures")(
        test("should return unprocessedItems in Zipped failure case") {
          val autoBatched = putItem("mockBatches", itemOne) zip putItem("mockBatches", itemTwo)
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchWriteError(
                ScalaMap("mockBatches" -> Chunk(DynamoDBBatchError.Put(itemOne)))
              )
            )
          )
        }.provideLayer(failedPartialMockBatchWriteTwoItems >>> DynamoDBExecutor.live) @@ TestAspect.withLiveClock,
        test("should return unprocessedItems in forEach failure case") {
          val autoBatched = forEach(List(itemOne, itemTwo))(item => putItem("mockBatches", item))
          val programExit = for {
            exit <- autoBatched.execute.exit
          } yield exit
          assertZIO(programExit)(
            fails(
              assertDynamoDBBatchWriteError(
                ScalaMap("mockBatches" -> Chunk(DynamoDBBatchError.Put(itemOne)))
              )
            )
          )
        }.provideLayer(failedPartialMockBatchWriteTwoItems >>> DynamoDBExecutor.live) @@ TestAspect.withLiveClock
      )
    )

  private val batchRetries = suite("Batch retries")(
    batchGetSuite,
    batchWriteSuite
  )
}