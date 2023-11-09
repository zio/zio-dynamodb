package zio.dynamodb

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.ZLayer
import zio.aws.core.config
import zio.aws.dynamodb
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty
import zio.dynamodb.DynamoDBExecutor

import java.net.URI
import zio.ZIO
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.KeySchema
import zio.dynamodb.BillingMode
import zio.dynamodb.AttributeDefinition

object DynamoDB {
  val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  val dynamoDBExecutorLayer = dynamoDbLayer >>> DynamoDBExecutor.live

  // Student table has both a partition key and a sort key
  val studentTableLayer: ZLayer[DynamoDBExecutor, Throwable, Unit] =
    ZLayer.scoped(
      ZIO.acquireRelease(acquire =
        DynamoDBQuery
          .createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
            AttributeDefinition.attrDefnString("email"),
            AttributeDefinition.attrDefnString("subject")
          )
          .execute
      )(release = _ => DynamoDBQuery.deleteTable("student").execute.orDie)
    )

  // Person table has only a partition key
  val personTableLayer: ZLayer[DynamoDBExecutor, Throwable, Unit] =
    ZLayer.scoped(
      ZIO.acquireRelease(acquire =
        DynamoDBQuery
          .createTable("person", KeySchema("id"), BillingMode.PayPerRequest)(
            AttributeDefinition.attrDefnString("id")
          )
          .execute
      )(release = _ => DynamoDBQuery.deleteTable("person").execute.orDie)
    )

}
