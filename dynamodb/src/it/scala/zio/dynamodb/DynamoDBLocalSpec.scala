package zio.dynamodb

import zio.test.ZIOSpec
import zio.ZLayer

abstract class DynamoDBLocalSpec extends ZIOSpec[DynamoDBExecutor] {
  override def bootstrap: ZLayer[Any, Nothing, DynamoDBExecutor] = DynamoDBLocal.dynamoDBExecutorLayer.orDie
}
