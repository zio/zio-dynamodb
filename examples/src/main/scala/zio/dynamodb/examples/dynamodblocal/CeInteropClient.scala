package zio.dynamodb.examples.dynamodblocal

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

import cats.effect.std.Console
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
import zio.dynamodb.DynamoDBQuery
import zio.stream.ZStream
import fs2.Pure

/**
 * example interop app
 *
 * to run in the sbt console:
 * {{{
 * zio-dynamodb-examples/runMain zio.dynamodb.examples.dynamodblocal.CeInteropClient
 * }}}
 */
object CeInteropClient extends IOApp.Simple {
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

  val fs2Stream: fs2.Stream[Pure, Int]      = fs2.Stream(1, 2, 3)
  val zioStream: ZStream[Any, Nothing, Int] = ZStream(1, 2, 3)

  import zio.stream.interop.fs2z._

  val fs = zioStream.toFs2Stream
  // Cannot find an implicit Compiler[[x]zio.ZIO[Any,Nothing,x], G]. This typically means you need a
  // Concurrent[[x]zio.ZIO[Any,Nothing,x]] in scopebloop
  //val x                                       = zioStream.toFs2Stream.compile.drain

//  val zioStream2: ZStream[Any, Throwable, Int] = ???

  val run = {
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.of requires an implicit Runtime

    for {
      _ <- DynamoDBExceutorF
             .of[IO](commonAwsConfig) { builder =>
               builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
             }
             .use { implicit dynamoDBExecutorF => // To use extension method we need implicit here
               for {
                 _         <- fs2Stream.compile.drain // .evalTap(i => std.Console[IO].println(s"i=$i"))
                 _         <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                                AttributeDefinition.attrDefnString("id")
                              ).executeToF
                 _         <- put("Person", Person(id = "avi", name = "Avinder")).executeToF
                 result    <- get("Person")(Person.id.partitionKey === "avi").executeToF
                 zioStream <- DynamoDBQuery.scanAll[Person]("Person").executeToF
                 _         <- Console[IO].println(s"XXXXXX result=$result stream=$zioStream")
                 _         <- deleteTable("Person").executeToF
               } yield ()
             }
    } yield ()
  }

}
