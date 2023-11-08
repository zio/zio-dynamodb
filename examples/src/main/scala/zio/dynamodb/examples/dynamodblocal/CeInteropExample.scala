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
 * example cats effect interop application
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
  // TODO: def program[F[_]]
  val run = {
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.of requires an implicit Runtime

    for {
      _ <- DynamoDBExceutorF
             .ofCustomised[IO] { builder => // note only AWS SDK model is exposed here, not zio.aws
               builder
                 .endpointOverride(URI.create("http://localhost:8000"))
                 .region(Region.US_EAST_1)
                 .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
             }
             .use { implicit dynamoDBExecutorF => // To use extension method "executeToF" we need implicit here
               for {
                 _         <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                                AttributeDefinition.attrDefnString("id")
                              ).executeToF
                 _         <- put(tableName = "Person", Person(id = "avi", name = "Avinder")).executeToF
                 result    <- get(tableName = "Person")(Person.id.partitionKey === "avi").executeToF
                 _         <- Console[IO].println(s"found=$result")
                 fs2Stream <- DynamoDBQuery
                                .scanAll[Person](tableName = "Person")
                                .parallel(50)                                                        // server side parallel scan
                                .filter(Person.name.beginsWith("Avi") && Person.name.contains("de")) // reified optics
                                .executeToF
                 _         <- fs2Stream.evalTap(person => Console[IO].println(s"person=$person")).compile.drain
                 _         <- deleteTable("Person").executeToF
               } yield ()
             }
    } yield ()
  }

}
