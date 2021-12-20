package zio.dynamodb

import scala.collection.immutable.{ Map => ScalaMap }
import io.github.vigoo.zioaws.dynamodb
import io.github.vigoo.zioaws.dynamodb.model.BatchGetItemResponse
import zio.ULayer
import zio.dynamodb.DynamoDBQuery.{ forEach, getItem, BatchGetItem }
import zio.test.Assertion.equalTo
import zio.test.mock.Expectation.value
import zio.test._

object LiveExecutorSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    otherSuite.provideLayer(mockedBatchGet >>> DynamoDBExecutor.live)

  private val mockBatches  = "mockBatches"
  private val firstRequest =
    zio.dynamodb.DynamoDBExecutorImpl.generateBatchGetItemRequest(
      BatchGetItem(
        ScalaMap(
          TableName(mockBatches) -> BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("k1" -> "v1"), PrimaryKey("k1" -> "v2")),
            projectionExpressionSet = Set.empty
          )
        )
      )
    )

  private val retryRequest =
    zio.dynamodb.DynamoDBExecutorImpl.generateBatchGetItemRequest(
      BatchGetItem(
        ScalaMap(
          TableName(mockBatches) -> BatchGetItem.TableGet(
            keysSet = Set(PrimaryKey("k1" -> "v2")),
            projectionExpressionSet = Set.empty
          )
        )
      )
    )

  private val mockedBatchGet: ULayer[io.github.vigoo.zioaws.dynamodb.DynamoDb] = dynamodb.DynamoDb.DynamoDbMock
    .BatchGetItem(
      equalTo(firstRequest),
      value(
        BatchGetItemResponse(
          responses = Some(
            ScalaMap(
              mockBatches -> List(
                ScalaMap("k1" -> io.github.vigoo.zioaws.dynamodb.model.AttributeValue(s = Some("v1")))
              )
            )
          ),
          unprocessedKeys = Some(
            ScalaMap(
              mockBatches -> io.github.vigoo.zioaws.dynamodb.model.KeysAndAttributes(
                keys = List(ScalaMap("k1" -> io.github.vigoo.zioaws.dynamodb.model.AttributeValue(s = Some("v2"))))
              )
            )
          )
        ).asReadOnly
      )
    ) ++ dynamodb.DynamoDb.DynamoDbMock
    .BatchGetItem(
      equalTo(retryRequest),
      value(
        BatchGetItemResponse(
          responses = Some(
            ScalaMap(
              mockBatches -> List(
                ScalaMap(
                  "k1" -> io.github.vigoo.zioaws.dynamodb.model.AttributeValue(s = Some("v2")),
                  "k2" -> io.github.vigoo.zioaws.dynamodb.model.AttributeValue(s = Some("v23"))
                )
              )
            )
          ),
          unprocessedKeys = None
        ).asReadOnly
      )
    )

  private val otherSuite =
    suite("with mocks")(
      testM("should retry when there are unprocessed keys") {
        for {
          response <- forEach(1 to 2) { i =>
                        getItem(mockBatches, PrimaryKey("k1" -> s"v$i"))
                      }.execute

        } yield assert(response)(equalTo(List(Some(Item("k1" -> "v1")), Some(Item("k1" -> "v2", "k2" -> "v23")))))
      }
    )
}
