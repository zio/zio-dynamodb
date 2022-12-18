package zio.dynamodb.examples.dynamodblocal

import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.ZLayer
import zio.aws.core.config
import zio.aws.dynamodb.DynamoDb
import zio.aws.{ dynamodb, netty }
import zio.dynamodb.DynamoDBExecutor
import zio.dynamodb.examples.LocalDdbServer

import java.net.URI

object DynamoDB {
  val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  val layer =
    (dynamoDbLayer >>> DynamoDBExecutor.live) ++ LocalDdbServer.inMemoryLayer
}
