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
                        retryPolicy = Schedule.recurs(1)
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

  private val clockLayer = ZLayer.identity[Has[Clock.Service]]

  private val batchRetries = suite("Batch retries")(
    batchGetSuite.provideCustomLayer((mockedBatchGet ++ clockLayer) >>> DynamoDBExecutor.live),
    batchWriteSuite.provideCustomLayer((mockedBatchWrite ++ clockLayer) >>> DynamoDBExecutor.live)
  )
}
