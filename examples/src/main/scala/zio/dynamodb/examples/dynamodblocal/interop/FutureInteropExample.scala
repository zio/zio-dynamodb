package zio.dynamodb.examples.dynamodblocal.interop

import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.examples.dynamodblocal.interop.CeInteropExample.Person
import zio.dynamodb.{ AttributeDefinition, BillingMode, DynamoDBQuery, KeySchema }
import zio.dynamodb.interop.future.DynamoDBExecutorF

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
object FutureInteropExample extends App {
  val ddbExec = DynamoDBExecutorF.make(
    buildNettyClient = identity,
    buildDynamoDbClient = b => {
      b.endpointOverride(java.net.URI.create("http://localhost:8000"))
        .region(software.amazon.awssdk.regions.Region.US_EAST_1)
        .credentialsProvider(
          software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
            software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("dummy", "dummy")
          )
        )
    }
  )

  val program = for {
    _      <- ddbExec.execute(
                DynamoDBQuery.createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                  AttributeDefinition.attrDefnString("id")
                )
              )
    _      <- ddbExec.execute(put(tableName = "Person", Person(id = "avi", name = "Avinder")))
    result <- ddbExec.execute(get(tableName = "Person")(Person.id.partitionKey === "avi"))
    _       = println(s"found=$result")
    _      <- ddbExec.execute(DynamoDBQuery.deleteTable("Person"))

  } yield ()

  Await.result(program, 10.seconds)
}
