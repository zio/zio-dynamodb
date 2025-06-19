package zio.dynamodb.examples.dynamodblocal.interop

import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.examples.dynamodblocal.interop.CeInteropExample.Person
import zio.dynamodb.interop.future.DynamoDBExecutorF
import zio.dynamodb.interop.future.syntax._
import zio.dynamodb.{ AttributeDefinition, BillingMode, DynamoDBQuery, KeySchema }

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * example future interop application
 *
 * to run in the sbt console:
 * {{{
 * zio-dynamodb-examples/runMain zio.dynamodb.examples.dynamodblocal.interop.FutureInteropExample
 * }}}
 */
object FutureInteropExample extends App {
  implicit val ddbExec: DynamoDBExecutorF = DynamoDBExecutorF.make(
    buildNettyClient = identity,
    buildDynamoDbClient = _.endpointOverride(java.net.URI.create("http://localhost:8000"))
      .region(software.amazon.awssdk.regions.Region.US_EAST_1)
      .credentialsProvider(
        software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
          software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("dummy", "dummy")
        )
      )
  )

  val program          = for {
    _      <- DynamoDBQuery
                .createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                  AttributeDefinition.attrDefnString("id")
                )
                .executeToF
    _      <- put(tableName = "Person", Person(id = "avi", name = "Avinder")).executeToF
    result <- get(tableName = "Person")(Person.id.partitionKey === "avi").executeToF
    _       = println(s"found=$result")
    _      <- DynamoDBQuery.deleteTable("Person").executeToF

  } yield ()
  val programWithClose = program.andThen { case _ => ddbExec.close() }

  Await.result(programWithClose, 30.seconds)
}
