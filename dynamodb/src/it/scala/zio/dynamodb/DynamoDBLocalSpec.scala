package zio.dynamodb

import zio.test.ZIOSpec

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.ZLayer
import zio.aws.core.config
import zio.aws.dynamodb
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty

import java.net.URI

abstract class DynamoDBLocalSpec extends ZIOSpec[DynamoDBExecutor] {

  private lazy val awsConfig = ZLayer.succeed {
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
      endpointOverride = None,
      commonClientConfig = None
    )
  }

  private lazy val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private lazy val dynamoDBExecutorLayer = dynamoDbLayer >>> DynamoDBExecutor.live

  override def bootstrap: ZLayer[Any, Nothing, DynamoDBExecutor] = dynamoDBExecutorLayer.orDie
}
