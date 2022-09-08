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
import zio.stream.Stream
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
    address: Option[Address] = None
  )
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema                                          = DeriveSchema.gen[Student]
    val (email, subject, enrollmentDate, payment, address)       = ProjectionExpression.accessors[Student]
    // TODO: Avi - confirm we have expected PE type
    val expected: ProjectionExpression[Student, Option[Instant]] = enrollmentDate
    println(expected)
    val ce: FilterExpression[Student]                            = expected === Some(Instant.now)
    val ceAnd: FilterExpression[Student]                         = expected === Some(Instant.now) && expected === Some(Instant.now)
    val ceOr: FilterExpression[Student]                          = expected === Some(Instant.now) || expected === Some(Instant.now)
    val ceNot: FilterExpression[Student]                         = !(expected === Some(Instant.now) && expected === Some(Instant.now))
  }

  final case class Elephant(
    email: String,
    subject: String
  )
  object Elephant {
    implicit val schema  = DeriveSchema.gen[Elephant]
    val (email, subject) = ProjectionExpression.accessors[Elephant]
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
    avi         = Student("avi@gmail.com", "maths", Some(enrolDate), Payment.DebitCard)
    adam        = Student("adam@gmail.com", "english", Some(enrolDate), Payment.CreditCard)
    _          <- batchWriteFromStream(Stream(avi, adam)) { student =>
                    put("student", student)
                  }.runDrain
    _          <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _          <- batchReadFromStream("student", Stream(avi, adam))(s => PrimaryKey("email" -> s.email, "subject" -> s.subject))
                    .tap(student => console.putStrLn(s"student=$student"))
                    .runDrain
    /*
sealed trait CanFilter[A, -B]
object CanFilter {
  implicit def subtypeCanFilter[A, B](implicit ev: B <:< A): CanFilter[A, B]                          = new CanFilter[A, B] {}
  implicit def subtypeStreamCanFilter[A, B](implicit ev: B <:< A): CanFilter[A, Stream[Throwable, B]] =
    new CanFilter[A, Stream[Throwable, B]] {}
}
could not find implicit value for parameter ev: CanFilter[Option[java.time.Instant],zio.stream.ZStream[Any,Throwable, Student]]
     */
    _          <- scanAll[Student]("student")
                    .filter[Student] {                     // [Stream[Throwable, Student]]
                      // take a look at === and && and see why we do not get a ConditionExpression[Student]
                      // could be that === && ops are not holding on to type information
//                    (enrollmentDate === Some(enrolDate)) //: ConditionExpression[Option[Instant]]
                      (enrollmentDate === Some(
                        enrolDate
                      ) && payment === Payment.CreditCard) //: ConditionExpression[Student]
//                      Elephant.email === "elephant@gmail.com"
                    }
                    .execute
                    .map(_.runCollect)
    _          <- queryAll[Student]("student")
                    .filter( // [Stream[Throwable, Student]]
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
                    .tap(_.tap(student => console.putStrLn(s"scanAll - student=$student")).runDrain)
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
