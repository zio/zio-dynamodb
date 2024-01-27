package zio.dynamodb.examples

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.ZIOAppDefault
import zio.ZLayer
import zio.aws.core.config
import zio.aws.dynamodb
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty
import zio.dynamodb.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.get
import zio.dynamodb.DynamoDBQuery.put
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.syntax._
import zio.schema.DeriveSchema
import zio.schema.Schema

import java.net.URI
import zio.ZIO
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.KeySchema
import zio.dynamodb.BillingMode
import zio.dynamodb.AttributeDefinition

/**
 * Stand alone example using DynamoDBLocal.
 *
 * To run: sbt "zio-dynamodb-examples/runMain zio.dynamodb.examples.DynamoDBLocalMain"
 *
 * For more comprehensive examples see integration tests under zio-dynamodb/src/it/scala/zio/dynamodb
 */
object DynamoDBLocalMain extends ZIOAppDefault {
  private val studentTableLayer: ZLayer[DynamoDBExecutor, Throwable, Unit] =
    ZLayer.scoped(
      ZIO.acquireRelease(acquire =
        DynamoDBQuery
          .createTable("person", KeySchema("id"), BillingMode.PayPerRequest)(
            AttributeDefinition.attrDefnNumber("id")
          )
          .execute
      )(release = _ => DynamoDBQuery.deleteTable("person").execute.orDie)
    )

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema.CaseClass2[Int, String, Person] = DeriveSchema.gen[Person]

    val (id, firstName) = ProjectionExpression.accessors[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _           <- put("person", examplePerson).execute
    maybePerson <- get("person")(Person.id.partitionKey === 1).execute.maybeFound
    _           <- zio.Console.printLine(s"hello $maybePerson")
  } yield ()

  override def run =
    program.provide(dynamoDbLayer, studentTableLayer, DynamoDBExecutor.live)

}
