package zio.dynamodb

import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.DynamoDb.DynamoDbMock
import io.github.vigoo.zioaws.dynamodb.model.{
  BatchWriteItemResponse,
  AttributeValue => ZIOAwsAttributeValue,
  BatchGetItemResponse => ZIOAwsBatchGetItemResponse,
  KeysAndAttributes => ZIOAwsKeysAndAttributes
}
import zio.clock.Clock
import zio.{ Has, Schedule, ULayer, ZLayer }
import zio.dynamodb.DynamoDBQuery._

import scala.collection.immutable.{ Map => ScalaMap }
import zio.test.Assertion._
import zio.test.mock.Expectation.value
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }

object ExecutorSpec extends DefaultRunnableSpec with DynamoDBFixtures {

  override def spec: ZSpec[Environment, Failure] =
    suite("Executor spec")(
      batchRetries
    )

  private val mockBatches     = "mockBatches"
  private val itemOne         = Item("k1" -> "v1")
  private val clockLayer      = ZLayer.identity[Has[Clock.Service]]
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
    retryPolicy = Schedule.recurs(1)
  )

  private val firstWriteRequest =
    DynamoDBExecutorImpl.generateBatchWriteItem(
      batchWriteRequest
    )

  private val failedMockBatchGet: ULayer[DynamoDb] = DynamoDbMock
    .BatchGetItem(
      equalTo(firstGetRequest),
      value(
        ZIOAwsBatchGetItemResponse(
          unprocessedKeys = Some(
            ScalaMap(
              mockBatches -> ZIOAwsKeysAndAttributes(
                keys = List(
                  ScalaMap("k1" -> ZIOAwsAttributeValue(s = Some("v2"))),
                  ScalaMap("k1" -> ZIOAwsAttributeValue(s = Some("v1")))
                )
              )
            )
          )
        ).asReadOnly
      )
    )
    .atMost(2)

  private val successfulMockBatchGet: ULayer[DynamoDb] = DynamoDbMock
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

  private val getRequestItems: ScalaMap[TableName, BatchGetItem.TableGet] = ScalaMap(
    TableName(mockBatches) -> BatchGetItem.TableGet(
      Set(PrimaryKey("k1" -> "v1"), PrimaryKey("k1" -> "v2")),
      Set.empty
    )
  )
  private val batchGetItem: BatchGetItem                                  = BatchGetItem(
    requestItems = getRequestItems,
    retryPolicy = Schedule.recurs(1)
  )
  private val batchGetSuite                                               =
    suite("retry batch gets")(
      suite("successful batch gets")(testM("should retry when there are unprocessed keys") {
        for {
          response <- batchGetItem.execute
        } yield assert(response.responses.get(TableName(mockBatches)))(
          equalTo(Some(Set(itemOne, Item("k1" -> "v2", "k2" -> "v23"))))
        )
      }).provideCustomLayer((successfulMockBatchGet ++ clockLayer) >>> DynamoDBExecutor.live),
      suite("failed batch gets")(testM("should return keys we did not get") {
        for {
          response <- batchGetItem.execute
        } yield assert(response.unprocessedKeys)(
          equalTo(
            getRequestItems
          )
        )
      }).provideCustomLayer((failedMockBatchGet ++ clockLayer) >>> DynamoDBExecutor.live)
    )

  private val itemOneWriteRequest                    = Set(
    DynamoDBExecutorImpl.batchItemWriteToZIOAwsWriteRequest(BatchWriteItem.Put(itemOne))
  )
  private val failedMockBatchWrite: ULayer[DynamoDb] = DynamoDbMock
    .BatchWriteItem(
      equalTo(firstWriteRequest),
      value(
        BatchWriteItemResponse(
          unprocessedItems = Some(
            ScalaMap(
              mockBatches -> itemOneWriteRequest
            )
          )
        ).asReadOnly
      )
    )
    .atMost(2)

  private val successfulMockBatchWrite: ULayer[DynamoDb] = DynamoDbMock
    .BatchWriteItem(
      equalTo(firstWriteRequest),
      value(
        BatchWriteItemResponse(
          unprocessedItems = Some(
            ScalaMap(
              mockBatches -> itemOneWriteRequest
            )
          )
        ).asReadOnly
      )
    ) ++ DynamoDbMock.BatchWriteItem(
    equalTo(firstWriteRequest),
    value(
      BatchWriteItemResponse().asReadOnly
    )
  )

  private val batchWriteSuite =
    suite("retry batch writes")(
      suite("failed batch write")(
        testM("should retry when there are unprocessedItems and return unprocessedItems in failure case") {
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
      ).provideCustomLayer((failedMockBatchWrite ++ clockLayer) >>> DynamoDBExecutor.live),
      suite("successful batch write")(testM("should return no unprocessedItems") {
        for {
          response <- batchWriteRequest.execute
        } yield assert(response.unprocessedItems)(isNone)
      }).provideCustomLayer((successfulMockBatchWrite ++ clockLayer) >>> DynamoDBExecutor.live)
    )

  private val batchRetries = suite("Batch retries")(
    batchGetSuite,
    batchWriteSuite
  )
}
