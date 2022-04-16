package zio.dynamodb.examples.dynamodblocal

import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.aws.core.config
import zio.aws.dynamodb.DynamoDb
import zio.aws.{ dynamodb, http4s }
import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.DynamoDBQuery.{ createTable, put }
import zio.dynamodb._
import zio.dynamodb.examples.LocalDdbServer
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema, Schema }
import zio.stream.ZStream
import zio.{ Console, ZIO, ZIOAppDefault, ZLayer }

import java.net.URI
import java.time.Instant

/**
 * An equivalent app to [[StudentJavaSdkExample]] but using `zio-dynamodb` - note the reduction in boiler plate code!
 */
object StudentZioDynamoDbExample extends ZIOAppDefault {

  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment
  }
  final case class Student(email: String, subject: String, enrollmentDate: Option[Instant], payment: Payment)
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema: Schema[Student] = DeriveSchema.gen[Student]
  }

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (http4s.Http4sClient.default ++ awsConfig) >>> config.AwsConfig.default >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val customLayer =
    (dynamoDbLayer  >>> DynamoDBExecutor.live) ++ LocalDdbServer.inMemoryLayer

  private val program = for {
    _              <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
                        AttributeDefinition.attrDefnString("email"),
                        AttributeDefinition.attrDefnString("subject")
                      ).execute
    enrollmentDate <- ZIO.attempt(Instant.parse("2021-03-20T01:39:33Z"))
    avi             = Student("avi@gmail.com", "maths", Some(enrollmentDate), Payment.DebitCard)
    adam            = Student("adam@gmail.com", "english", Some(enrollmentDate), Payment.CreditCard)
    _              <- batchWriteFromStream(ZStream(avi, adam)) { student =>
                        put("student", student)
                      }.runDrain
    _              <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _              <- batchReadFromStream("student", ZStream(avi, adam))(s => PrimaryKey("email" -> s.email, "subject" -> s.subject))
                        .tap(student => Console.printLine(s"student=$student"))
                        .runDrain
  } yield ()

  override def run = program.provide(customLayer)
}
