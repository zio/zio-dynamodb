package zio.dynamodb.examples.dynamodblocal

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.blocking.Blocking
import zio.clock.Clock
import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.DynamoDBQuery.{ createTable, put }
import zio.dynamodb._
import zio.dynamodb.examples.LocalDdbServer
import zio.dynamodb.examples.dynamodblocal.StudentJavaSdkExample.parseInstant
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }
import zio.stream.ZStream
import zio.{ console, App, ExitCode, Has, URIO, ZIO, ZLayer }

import java.net.URI
import java.time.Instant

object StudentZioDynamoDbExample extends App {

  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment
  }
  final case class Student(email: String, subject: String, enrollmentDate: Option[Instant], payment: Payment)
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema = DeriveSchema.gen[Student]
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
    (http4s.default ++ awsConfig) >>> config.configured() >>> dynamodb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val layer =
    (dynamoDbLayer ++ ZLayer
      .identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live ++ (Blocking.live >>> LocalDdbServer.inMemoryLayer)

  val program = for {
    _              <- createTable("tableName", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
                        AttributeDefinition.attrDefnString("email"),
                        AttributeDefinition.attrDefnString("subject")
                      ).execute
    enrollmentDate <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
    avi             = Student("avi@gmail.com", "maths", Some(enrollmentDate), Payment.DebitCard)
    adam            = Student("adam@gmail.com", "english", Some(enrollmentDate), Payment.CreditCard)
    _              <- batchWriteFromStream(ZStream(avi, adam)) { student =>
                        put("tableName", student)
                      }.runDrain
    _              <- put("tableName", avi.copy(payment = Payment.CreditCard)).execute
    _              <- batchReadFromStream("tableName", ZStream(avi, adam))(s =>
                        PrimaryKey("email" -> s.email, "subject" -> s.subject)
                      ).tap(student => console.putStrLn(s"student=$student")).runDrain
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
