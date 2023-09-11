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
import java.net.URI

import zio.dynamodb.interop.ce.syntax._
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.ProjectionExpression
import zio.schema.DeriveSchema
import zio.dynamodb.DynamoDBError
import zio.Scope
import zio.{ ZEnvironment, ZIO }
import cats.effect.kernel.Async
import zio.ULayer
import zio.aws.core.httpclient.HttpClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder

/**
 * example interop app
 */
object InteropClient extends IOApp.Simple {
  val awsConfig: ULayer[config.CommonAwsConfig]       = ZLayer.succeed(
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
  val layer: ZLayer[Any, Throwable, DynamoDBExecutor] = dynamoDbLayer >>> DynamoDBExecutor.live

  val x: ZIO[Any with Scope, Throwable, ZEnvironment[DynamoDBExecutor]] = layer.build
  val x2: ZIO[Any with Scope, Throwable, DynamoDBExecutor]              = for { // problem is we have Scope capability
    env            <- x
    dynamoDBExector = env.get[DynamoDBExecutor]
  } yield dynamoDBExector
  val x3                                                                =  {
    x2
  }

  // ZIO
  // =================================================================================================
  // CE

  def withDynamoDBScope[F[_]]( // fatten out layer dependencies
    awsCommonConfig: config.CommonAwsConfig,
    httpClient: HttpClient,
    customization: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder
  )(dynamoDbProgram: F[_])(implicit
    F: Async[F]
  ): F[_] =
    // re-assemble ZLayer stuff here
    // invoke ZIO DynamoDB callback program somehow in context of this ZLayer
    ???

  final case class Person(id: String, name: String)
  object Person {
    implicit val schema = DeriveSchema.gen[Person]
    val (id, name)      = ProjectionExpression.accessors[Person]
  }

  val run = {
    val query                                     = DynamoDBQuery.get("table")(Person.id.partitionKey === "avi")
    val result: IO[Either[DynamoDBError, Person]] = query.executeToF[IO]
    result.map(r => println(s"query result = $r"))
  }
}
