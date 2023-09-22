package `zio.dynamodb.interop.ce`

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.ZLayer
import zio.aws.core.config
import zio.aws.dynamodb
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty
import zio.dynamodb.DynamoDBExecutor

import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all._

import java.net.URI

import zio.dynamodb.interop.ce.syntax._
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.ProjectionExpression
import zio.schema.DeriveSchema
import zio.dynamodb.DynamoDBError
import zio.ULayer

/**
 * example interop app
 */
object InteropClient extends IOApp.Simple {
  val commonAwsConfig = config.CommonAwsConfig(
    region = None,
    credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
    endpointOverride = None,
    commonClientConfig = None
  )

  val awsConfig: ULayer[config.CommonAwsConfig]       = ZLayer.succeed(commonAwsConfig)
  val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }
  val layer: ZLayer[Any, Throwable, DynamoDBExecutor] = dynamoDbLayer >>> DynamoDBExecutor.live

  // ZIO
  // =================================================================================================
  // CE

  final case class Person(id: String, name: String)
  object Person {
    implicit val schema = DeriveSchema.gen[Person]
    val (id, name)      = ProjectionExpression.accessors[Person]
  }

  val run = {
    implicit val runtime = zio.Runtime.default

    for {
      _ <- DynamoDBExceutorF.of[IO](commonAwsConfig).use { ddbe =>
             val query                                     = DynamoDBQuery.get("table")(Person.id.partitionKey === "avi")
             val result: IO[Either[DynamoDBError, Person]] = ddbe.execute(query)
             result
           }
    } yield ()
  }
}
