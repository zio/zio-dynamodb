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
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema, Schema }
import zio.stream.ZStream
import zio.{ console, App, ExitCode, Has, URIO, ZIO, ZLayer }

import java.net.URI
import java.time.Instant

/**
 * An equivalent app to [[StudentJavaSdkExample]] but using `zio-dynamodb` - note the reduction in boiler plate code!
 */
object StudentZioDynamoDbExample2 extends App {

  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    // report bug
    // weirdly the compiler complains about order - maybe auto derivation sorts the cases
    val schema: Schema.Enum3[CreditCard.type, DebitCard.type, PayPal.type, Payment] = DeriveSchema.gen[Payment]
  }
  final case class Student(email: String, subject: String, enrollmentDate: Option[Instant], payment: Payment)
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema: Schema.CaseClass4[String, String, Option[Instant], Payment, Student] =
      DeriveSchema.gen[Student]
  }

  object Foo extends DefaultJavaTimeSchemas

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

  val (email, subject, enrollmentDate, payment) = ProjectionExpression.accessors[Student]
  println(s"$email $subject $enrollmentDate $payment")

  val x: Either[String, KeyConditionExpression] = KeyConditionExpression(
    email === "avi@gmail.com" && subject === "maths"
  )
  println(s"XXXXXXXXXXXXXXXXXXXXX x=$x")

  private val program = for {
    _         <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
                   AttributeDefinition.attrDefnString("email"),
                   AttributeDefinition.attrDefnString("subject")
                 ).execute
    enrolDate <- ZIO.effect(Instant.parse("2021-03-20T01:39:33Z"))
    avi        = Student("avi@gmail.com", "maths", Some(enrolDate), Payment.DebitCard)
    adam       = Student("adam@gmail.com", "english", Some(enrolDate), Payment.CreditCard)
    _         <- batchWriteFromStream(ZStream(avi, adam)) { student =>
                   put("student", student)
                 }.runDrain
    _         <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _         <- batchReadFromStream("student", ZStream(avi, adam))(s => PrimaryKey("email" -> s.email, "subject" -> s.subject))
                   .tap(student => console.putStrLn(s"student=$student"))
                   .runDrain

    _         <- queryAll[Student]("student")
                   .filter(                                                              // Scan/Query
                     (enrollmentDate === Instant.now.toString) && (payment === "PayPal") // TODO: can we make values type safe?
                   )
                   .execute
    _         <- queryAll[Student]("student")
                   .filter(                                                              // Scan/Query
                     (enrollmentDate === Instant.now.toString) && (payment === "PayPal")
                   )
                   .whereKey(email === "avi@gmail.com" && subject === "maths")
                   .execute
    _         <- put[Student]("student", avi)
                   .where(
                     (enrollmentDate === Instant.now.toString) && (payment === "PayPal")
                   )
                   .execute
    _         <- updateItem("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")) {
                   import Foo._
                   enrollmentDate.set3(Some(Instant.now)) + payment.set2[Payment](Payment.PayPal)(Payment.schema)
                 }.execute
    _         <- deleteItem("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths"))
                   .where(
                     (enrollmentDate === Instant.now.toString) && (payment === "PayPal")
                   )
                   .execute
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
