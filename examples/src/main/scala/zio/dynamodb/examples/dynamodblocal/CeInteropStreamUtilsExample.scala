package zio.dynamodb.examples.dynamodblocal

import cats.effect.IO
import cats.effect.IOApp
import cats.effect.kernel.Async
import cats.effect.std.Console
import cats.effect.std.Dispatcher
import cats.syntax.all._
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.dynamodb.AttributeDefinition
import zio.dynamodb.BillingMode
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.KeySchema
import zio.dynamodb.PrimaryKey
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.interop.ce.syntax2._
import zio.schema.DeriveSchema

import java.net.URI

/**
 * example interop app
 *
 * to run in the sbt console:
 * {{{
 * zio-dynamodb-examples/runMain zio.dynamodb.examples.dynamodblocal.CeInteropStreamUtilsExample
 * }}}
 */
object CeInteropStreamUtilsExample extends IOApp.Simple {

  final case class Person(id: String, name: String)
  object Person {
    implicit val schema = DeriveSchema.gen[Person]
    val (id, name)      = ProjectionExpression.accessors[Person]
  }

  def program[F[_]](implicit F: Async[F]) = {
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.ofXXX requires an implicit Runtime

    val dynamoDBExceutorF = DynamoDBExceutorF
      .ofCustomised[F] { builder =>
        builder
          .endpointOverride(URI.create("http://localhost:8000"))
          .region(Region.US_EAST_1)
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
      }

    val resources = for {
      dynamo     <- dynamoDBExceutorF
      dispatcher <- Dispatcher.parallel[F] // required by batchReadXXX and batchWriteXXX utilities
    } yield (dynamo, dispatcher)

    for {
      _ <- resources.use { t =>
             implicit val dynamo     = t._1 // To use executeToF extension method we need this implicit here
             implicit val dispatcher = t._2

             val fs2Input   = fs2.Stream(Person("avi", "avi")).covary[F]
             val x: F[Unit] = batchWriteFromStreamF(fs2Input)(p => put("Person", p)).compile.drain
             val xs2        = batchReadItemFromStreamF("Person", fs2Input) { p =>
               PrimaryKey("id" -> p.id)
             }.compile.toList

             for {
               _         <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                              AttributeDefinition.attrDefnString("id")
                            ).executeToF
               fs2Input   = fs2.Stream(Person("avi", "avi")).covary[F]
               _         <- batchWriteFromStreamF(fs2Input)(p => put("Person", p)).compile.drain
               console    = Console.make[F]
               fs2Stream <- scanAll[Person]("Person").executeToF
               _         <- fs2Stream.evalTap(p => console.println(s"scanned $p")).compile.drain
               _         <- batchReadFromStreamF("Person", fs2Input) { p =>
                              Person.id.partitionKey === p.id
                            }.evalTap(p => console.println(s"person $p")).compile.toList
               _         <- batchReadItemFromStreamF("Person", fs2Input) { p =>
                              PrimaryKey("id" -> p.id)
                            }.evalTap(item => console.println(s"item $item")).compile.toList
               _         <- deleteTable("Person").executeToF
             } yield ()
           }
    } yield ()
  }

  def run = program[IO]

}
