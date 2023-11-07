package zio.dynamodb.examples.dynamodblocal

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.dynamodb.DynamoDBQuery._

import cats.effect.std.Console
import cats.effect.kernel.Async
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
import cats.effect.std.Dispatcher

/**
 * example interop app
 *
 * to run in the sbt console:
 * {{{
 * zio-dynamodb-examples/runMain zio.dynamodb.examples.dynamodblocal.CeInteropExample
 * }}}
 */
object CeInteropExample2 extends IOApp.Simple {

  final case class Person(id: String, name: String)
  object Person {
    implicit val schema = DeriveSchema.gen[Person]
    val (id, name)      = ProjectionExpression.accessors[Person]
  }

  def program[F[_]](implicit F: Async[F]) = {
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.of requires an implicit Runtime

    val dynamoDBExceutorF = DynamoDBExceutorF
      .ofCustomised[F] { builder =>
        builder
          .endpointOverride(URI.create("http://localhost:8000"))
          .region(Region.US_EAST_1)
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
      }

    val resources = for {
      dy <- dynamoDBExceutorF
      d  <- Dispatcher.parallel[F]
    } yield (dy, d)

    for {
      _ <- resources.use { t => // To use extension method we need implicit here
             implicit val dy: DynamoDBExceutorF[F] = t._1
             implicit val d: Dispatcher[F]         = t._2

             for {
               _         <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                              AttributeDefinition.attrDefnString("id")
                            ).executeToF
               _         <- put("Person", Person(id = "avi", name = "Avinder")).executeToF
               result    <- get("Person")(Person.id.partitionKey === "avi").executeToF
               console    = Console.make[F]
               _         <- console.println(s"result=$result")
               _          = println(result)
               fs2Stream <- scanAll[Person]("Person").executeToF
               _         <- fs2Stream.evalTap(p => console.println(s"scanned $p")).compile.drain
               fs2Input   = fs2.Stream(Person("avi", "avi")).covary[F]
               xs        <- batchReadFromStreamF("Person", fs2Input) { p =>
                              Person.id.partitionKey === p.id
                            }.compile.toList
               _          = println(s"people: $xs")
               _         <- deleteTable("Person").executeToF
             } yield ()
           }
    } yield ()
  }

  def run = program[IO]

}
