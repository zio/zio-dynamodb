package zio.dynamodb

import scala.collection.immutable.{ Map => ScalaMap }
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.dynamodb.DynamoDb.DynamoDbMock
import io.github.vigoo.zioaws.dynamodb.model.{
  AttributeValue,
  BatchGetItemResponse,
  BatchWriteItemResponse,
  KeysAndAttributes
}
import zio.ULayer
import zio.duration._
import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem }
import zio.test.Assertion.equalTo
import zio.test.mock.Expectation.value
import zio.test._
import zio.test.environment.TestEnvironment

object LiveExecutorSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("batch retries")(
      batchGetSuite.provideLayer(mockedBatchGet >>> DynamoDBExecutor.live),
      batchWriteSuite.provideLayer(mockedBatchWrite >>> DynamoDBExecutor.live)
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
        BatchGetItemResponse(
          responses = Some(
            ScalaMap(
              mockBatches -> List(
                ScalaMap("k1" -> AttributeValue(s = Some("v1")))
              )
            )
          ),
          unprocessedKeys = Some(
            ScalaMap(
              mockBatches -> KeysAndAttributes(
                keys = List(ScalaMap("k1" -> AttributeValue(s = Some("v2"))))
              )
            )
          )
        ).asReadOnly
      )
    ) ++ DynamoDbMock
    .BatchGetItem(
      equalTo(retryGetRequest),
      value(
        BatchGetItemResponse(
          responses = Some(
            ScalaMap(
              mockBatches -> List(
                ScalaMap(
                  "k1" -> AttributeValue(s = Some("v2")),
                  "k2" -> AttributeValue(s = Some("v23"))
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

}
