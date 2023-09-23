package zio.dynamodb.interop.ce

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.ZLayer
import zio.aws.core.config
import zio.aws.dynamodb
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty
import zio.dynamodb.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.{ createTable, deleteTable, get, put }

import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all._

import java.net.URI

import zio.dynamodb.interop.ce.syntax._
import zio.dynamodb.ProjectionExpression
import zio.schema.DeriveSchema
import zio.ULayer
import zio.dynamodb.KeySchema
import zio.dynamodb.BillingMode
import zio.dynamodb.AttributeDefinition

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
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.of requires an implicit Runtime

    for {
      _ <- DynamoDBExceutorF
             .of[IO](commonAwsConfig) { builder =>
               builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
             }
             .use { implicit dynamoDBExecutorF => // To use extension method we need implicit here
               for {
                 _      <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                             AttributeDefinition.attrDefnString("id")
                           ).executeToF
                 _      <- put("Person", Person(id = "avi", name = "Avinder")).executeToF
                 result <- get("Person")(Person.id.partitionKey === "avi").executeToF
                 _       = println(s"XXXXXX result=$result")
                 _      <- deleteTable("Person").executeToF
               } yield ()
             }
    } yield ()
  }

}
