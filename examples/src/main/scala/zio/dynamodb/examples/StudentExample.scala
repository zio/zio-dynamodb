package zio.dynamodb.examples

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.blocking.Blocking
import zio.clock.Clock
import zio.dynamodb.DynamoDBQuery.createTable
import zio.dynamodb.{ AttributeDefinition, BillingMode, DynamoDBExecutor, KeySchema }
import zio.{ App, ExitCode, Has, URIO, ZLayer }

import java.net.URI

object StudentExample extends App {

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (http4s.default ++ awsConfig) >>> config.configured() >>> dynamodb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val layer =
    (dynamoDbLayer ++ ZLayer
      .identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live ++ (Blocking.live >>> LocalDdbServer.inMemoryLayer)

  val program = for {
    _ <- createTable("tableName", KeySchema("id", "subject"), BillingMode.PayPerRequest)(
           AttributeDefinition.attrDefnNumber("id"),
           AttributeDefinition.attrDefnString("subject")
         ).execute
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
