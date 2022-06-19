package zio.dynamodb.examples.dynamodblocal

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.blocking.Blocking
import zio.clock.Clock
import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.examples.LocalDdbServer
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }
import zio.stream.ZStream
import zio.{ console, App, ExitCode, Has, URIO, ZIO, ZLayer }

import java.net.URI
import java.time.Instant

/**
 * An equivalent app to [[StudentJavaSdkExample]] but using `zio-dynamodb` - note the reduction in boiler plate code!
 * It also uses the type safe query and update API.
 */
object StudentZioDynamoDbExampleWithOptics extends App {

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
    altPayment: Payment,
    studentNumber: Int,
    collegeName: String,
    address: Option[Address] = None,
    addresses: List[Address] = List.empty[Address],
    groups: Set[String] = Set.empty[String]
  )
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema                                                                                               = DeriveSchema.gen[Student]
    val (email, subject, enrollmentDate, payment, altPayment, studentNumber, collegeName, address, addresses, groups) =
      ProjectionExpression.accessors[Student]
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

  private val layer = ((dynamoDbLayer ++ ZLayer.identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live) ++ (ZLayer
    .identity[Has[Blocking.Service]] >>> LocalDdbServer.inMemoryLayer)

  import zio.dynamodb.examples.dynamodblocal.StudentZioDynamoDbExampleWithOptics.Student._

  private val program = for {
    _          <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
                    AttributeDefinition.attrDefnString("email"),
                    AttributeDefinition.attrDefnString("subject")
                  ).execute
    enrolDate  <- ZIO.effect(Instant.parse("2021-03-20T01:39:33Z"))
    enrolDate2 <- ZIO.effect(Instant.parse("2022-03-20T01:39:33Z"))
    avi         = Student(
                    "avi@gmail.com",
                    "maths",
                    Some(enrolDate),
                    Payment.DebitCard,
                    Payment.CreditCard,
                    1,
                    "college1",
                    None,
                    List(Address("line2", "postcode2")),
                    Set("group1", "group2")
                  )
    adam        = Student(
                    "adam@gmail.com",
                    "english",
                    Some(enrolDate),
                    Payment.CreditCard,
                    Payment.DebitCard,
                    2,
                    "college1",
                    None,
                    List.empty,
                    Set(
                      "group1",
                      "group2"
                    )
                  )
    _          <- batchWriteFromStream(ZStream(avi, adam)) { student =>
                    put("student", student)
                  }.runDrain
    _          <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _          <- batchReadFromStream("student", ZStream(avi, adam))(student =>
                    PrimaryKey("email" -> student.email, "subject" -> student.subject)
                  )
                    .tap(student => console.putStrLn(s"student=$student"))
                    .runDrain
    _          <- scanAll[Student]("student")
                    .parallel(10)
                    .filter {
                      enrollmentDate === Some(enrolDate) && payment === Payment.PayPal
                    }
                    .execute
                    .map(_.runCollect)
    _          <- queryAll[Student]("student")
                    .filter(
                      enrollmentDate === Some(enrolDate) && payment === Payment.PayPal
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
                    altPayment.set(Payment.PayPal) + addresses.prependList(List(Address("line0", "postcode0"))) + studentNumber
                      .add(1000) + groups.addSet(Set("group3"))
                  }.execute
    _          <- updateItem("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")) {
                    altPayment.set(Payment.PayPal) + addresses.appendList(List(Address("line3", "postcode3"))) + groups
                      .deleteFromSet(Set("group1"))
                  }.execute
    _          <- updateItem("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")) {
                    enrollmentDate.setIfNotExists(Some(enrolDate2)) + payment.set(altPayment) + address
                      .set(
                        Some(Address("line1", "postcode1"))
                      )
                  }.execute
    _          <- updateItem("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")) {
                    addresses.remove(1)
                  }.execute
    _          <- deleteItem("student", PrimaryKey("email" -> "adam@gmail.com", "subject" -> "english"))
                    .where(
                      enrollmentDate === Some(enrolDate) && payment <> Payment.PayPal && studentNumber
                        .between(1, 3) && groups.contains("group1") && collegeName.contains(
                        "college1"
                      ) && collegeName.size > 1 && groups.size > 1
                    )
                    .execute
    _          <- scanAll[Student]("student")
                    .filter(payment.in(Payment.PayPal) && payment.inSet(Set(Payment.PayPal)))
                    .execute
                    .tap(_.tap(student => console.putStrLn(s"scanAll - student=$student")).runDrain)
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
