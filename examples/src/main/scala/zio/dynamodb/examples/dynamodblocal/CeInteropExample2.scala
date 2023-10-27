package zio.dynamodb.examples.dynamodblocal

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.dynamodb.DynamoDBQuery.{ createTable, deleteTable, get, put }

import cats.effect.std.Console
import cats.effect.IO
import cats.effect.IOApp

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

  val run = {
    implicit val runtime = zio.Runtime.default // DynamoDBExceutorF.of requires an implicit Runtime

//    implicit val async: Async[IO] = ???

    val dynamoDBExceutorF = DynamoDBExceutorF
      .ofCustomised[IO] { builder =>
        builder
          .endpointOverride(URI.create("http://localhost:8000"))
          .region(Region.US_EAST_1)
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
      }

    val x = for {
      dy <- dynamoDBExceutorF
      d  <- Dispatcher.parallel[IO]
    } yield (dy, d)

    // def program[F[_]](implicit F: Async[F]) =
    //   for {
    //     _                <- Console[IO].println(s" $F")
    //     dynamoDBExceutorF = DynamoDBExceutorF
    //                           .ofCustomised[F] { builder =>
    //                             builder
    //                               .endpointOverride(URI.create("http://localhost:8000"))
    //                               .region(Region.US_EAST_1)
    //                               .credentialsProvider(
    //                                 StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
    //                               )
    //                           }
    //     x                 = for {
    //                           dy <- dynamoDBExceutorF
    //                           d  <- Dispatcher.parallel[F]
    //                         } yield (dy, d)
    //     _                <- x.use { t => // To use extension method we need implicit here
    //                           implicit val dy               = t._1
    //                           implicit val d: Dispatcher[F] = t._2

    //                           for {
    //                             _       <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
    //                                          AttributeDefinition.attrDefnString("id")
    //                                        ).executeToF
    //                             _       <- put("Person", Person(id = "avi", name = "Avinder")).executeToF
    //                             result  <- get("Person")(Person.id.partitionKey === "avi").executeToF
    //                             _       <- Console[IO].println(s"XXXXXX result=$result")
    //                             fs2Input = fs2.Stream(Person("avi", "avi"))
    //                             x        = (p: Person) => Person.id.partitionKey === p.id
    //                             y       <- (
    //                                            batchReadFromStreamF[F, Person, Person]("Person", fs2Input) { p =>
    //                                              Person.id.partitionKey === p.id
    //                                            }
    //                                        ).compile.toList
    //                             _       <- Console[IO].println(s"XXXXXX $y")
    //                             _       <- deleteTable("Person").executeToF
    //                           } yield ()
    //                         }
    //   } yield ()

    for {
      _ <- x.use { t => // To use extension method we need implicit here
             implicit val dy                = t._1
             implicit val d: Dispatcher[IO] = t._2

             for {
               _       <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                            AttributeDefinition.attrDefnString("id")
                          ).executeToF
               _       <- put("Person", Person(id = "avi", name = "Avinder")).executeToF
               result  <- get("Person")(Person.id.partitionKey === "avi").executeToF
               _       <- Console[IO].println(s"XXXXXX result=$result")
               // diverging implicit expansion for type cats.effect.kernel.Async[F] starting with method asyncForKleisli in object Async
//               fs2Stream <- dy.execute(scanAll[Person]("Person"))
               fs2Input = fs2.Stream(Person("avi", "avi"))
               x        = (p: Person) => Person.id.partitionKey === p.id
               y       <- batchReadFromStreamF[IO, Person, Person]("Person", fs2Input) { p =>
                            Person.id.partitionKey === p.id
                          }.compile.toList
               _       <- Console[IO].println(s"XXXXXX $y")
               _       <- deleteTable("Person").executeToF
             } yield ()
           }
    } yield ()
  }

}
