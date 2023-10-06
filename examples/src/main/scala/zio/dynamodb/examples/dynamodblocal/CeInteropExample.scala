package zio.dynamodb.examples.dynamodblocal

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.dynamodb.DynamoDBQuery.{ createTable, deleteTable, get, put }

import cats.effect.std.Console
import cats.effect.IO
import cats.effect.IOApp

import java.net.URI

import zio.dynamodb.interop.ce.syntax._
import zio.dynamodb.ProjectionExpression
import zio.schema.DeriveSchema
import zio.dynamodb.KeySchema
import zio.dynamodb.BillingMode
import zio.dynamodb.AttributeDefinition
import zio.dynamodb.DynamoDBQuery

/**
 * example interop app
 *
 * to run in the sbt console:
 * {{{
 * zio-dynamodb-examples/runMain zio.dynamodb.examples.dynamodblocal.CeInteropExample
 * }}}
 */
object CeInteropExample extends IOApp.Simple {

  final case class Person(id: String, name: String)
  object Person {
    implicit val schema = DeriveSchema.gen[Person]
    val (id, name)      = ProjectionExpression.accessors[Person]
  }

  val run = {
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.of requires an implicit Runtime

    for {
      _ <- DynamoDBExceutorF
             .ofCustomised[IO] { builder =>
               builder
                 .endpointOverride(URI.create("http://localhost:8000"))
                 .region(Region.US_EAST_1)
                 .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
             }
             .use { implicit dynamoDBExecutorF => // To use extension method we need implicit here
               for {
                 _         <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                                AttributeDefinition.attrDefnString("id")
                              ).executeToF
                 _         <- put("Person", Person(id = "avi", name = "Avinder")).executeToF
                 result    <- get("Person")(Person.id.partitionKey === "avi").executeToF
                 fs2Stream <- DynamoDBQuery.scanAll[Person]("Person").executeToF
                 xs        <- fs2Stream.compile.toList
                 _         <- Console[IO].println(s"XXXXXX result=$result stream=$xs")
                 _         <- deleteTable("Person").executeToF
               } yield ()
             }
    } yield ()
  }

}
