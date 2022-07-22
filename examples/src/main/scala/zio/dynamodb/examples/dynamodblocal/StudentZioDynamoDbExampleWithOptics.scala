package zio.dynamodb.examples.dynamodblocal

import zio.aws.core.config
import zio.aws.dynamodb.DynamoDb
import zio.aws.{ dynamodb, netty }
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.examples.LocalDdbServer
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }
import zio.stream.ZStream
import zio.{ Console, ZIO, ZIOAppDefault, ZLayer }

import java.net.URI
import java.time.Instant

/**
 * An equivalent app to [[StudentJavaSdkExample]] but using `zio-dynamodb` - note the reduction in boiler plate code!
 * It also uses the type safe query and update API.
 */
object StudentZioDynamoDbExampleWithOptics extends ZIOAppDefault {

  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    implicit val schema = DeriveSchema.gen[Payment]
  }
  final case class Address(addr1: String, postcode: String)
  object Address {
    implicit val schema = DeriveSchema.gen[Address]
  }

  final case class Student(
    email: String,
    subject: String,
    enrollmentDate: Option[Instant],
    payment: Payment,
    address: Option[Address] = None
  )
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema                                    = DeriveSchema.gen[Student]
    val (email, subject, enrollmentDate, payment, address) = ProjectionExpression.accessors[Student]
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
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.configured() >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val customLayer = (dynamoDbLayer >>> DynamoDBExecutor.live) ++ LocalDdbServer.inMemoryLayer

  import zio.dynamodb.examples.dynamodblocal.StudentZioDynamoDbExampleWithOptics.Student._

  private val program = for {
    _          <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
                    AttributeDefinition.attrDefnString("email"),
                    AttributeDefinition.attrDefnString("subject")
                  ).execute
    enrolDate  <- ZIO.attempt(Instant.parse("2021-03-20T01:39:33Z"))
    enrolDate2 <- ZIO.attempt(Instant.parse("2022-03-20T01:39:33Z"))
    avi         = Student("avi@gmail.com", "maths", Some(enrolDate), Payment.DebitCard)
    adam        = Student("adam@gmail.com", "english", Some(enrolDate), Payment.CreditCard)
    _          <- batchWriteFromStream(ZStream(avi, adam)) { student =>
                    put("student", student)
                  }.runDrain
    _          <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _          <- batchReadFromStream("student", ZStream(avi, adam))(s => PrimaryKey("email" -> s.email, "subject" -> s.subject))
                    .tap(student => Console.printLine(s"student=$student"))
                    .runDrain
    _          <- scanAll[Student]("student").filter {
                    enrollmentDate === Some(enrolDate) && payment === Payment.CreditCard
                  }.execute.map(_.runCollect)
    _          <- queryAll[Student]("student")
                    .filter(
                      enrollmentDate === Some(enrolDate) && payment === Payment.CreditCard
                    )
                    .whereKey(email === "avi@gmail.com" && subject === "maths")
                    .execute
                    .map(_.runCollect)
    _          <- put[Student]("student", avi)
                    .where(
                      enrollmentDate === Some(enrolDate) && email === "avi@gmail.com" && payment === Payment.CreditCard
                    )
                    .execute
    _          <- updateItem("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")) {
                    enrollmentDate.set(Some(enrolDate2)) + payment.set(Payment.PayPal) + address
                      .set(
                        Some(Address("line1", "postcode1"))
                      )
                  }.execute
    _          <- deleteItem("student", PrimaryKey("email" -> "adam@gmail.com", "subject" -> "english"))
                    .where(
                      enrollmentDate === Some(enrolDate) && payment === Payment.CreditCard
                    )
                    .execute
    _          <- scanAll[Student]("student").execute
                    .tap(_.tap(student => Console.printLine(s"scanAll - student=$student")).runDrain)
  } yield ()

  override def run = program.provide(customLayer)
}
