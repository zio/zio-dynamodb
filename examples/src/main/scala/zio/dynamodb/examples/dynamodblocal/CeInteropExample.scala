package zio.dynamodb.examples.dynamodblocal

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.dynamodb.DynamoDBQuery.{ createTable, deleteTable, get, put }

import cats.effect.std.Console
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all._

import java.net.URI

import zio.dynamodb.interop.ce.syntax2._
import zio.dynamodb.ProjectionExpression
import zio.schema.DeriveSchema
import zio.dynamodb.KeySchema
import zio.dynamodb.BillingMode
import zio.dynamodb.AttributeDefinition
import zio.dynamodb.DynamoDBQuery
import cats.effect.kernel.Async

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

  def program[F[_]](implicit F: Async[F]) = {
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.of requires an implicit Runtime
    val console          = Console.make[F]

    for {
      _ <- DynamoDBExceutorF
             .ofCustomised[F] { builder => // note only AWS SDK model is exposed here, not zio.aws
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
                 _         <- console.println(s"found=$result")
                 fs2Stream <- DynamoDBQuery
                                .scanAll[Person](tableName = "Person")
                                .parallel(50)                                                        // server side parallel scan
                                .filter(Person.name.beginsWith("Avi") && Person.name.contains("de")) // reified optics
                                .executeToF
                 _         <- fs2Stream.evalTap(person => console.println(s"person=$person")).compile.drain
                 _         <- deleteTable("Person").executeToF
               } yield ()
             }
    } yield ()
  }

  val run = program[IO]
}
