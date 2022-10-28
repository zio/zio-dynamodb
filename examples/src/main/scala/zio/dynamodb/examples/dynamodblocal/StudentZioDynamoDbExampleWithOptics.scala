package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.stream.Stream
import zio.{ console, App, ExitCode, URIO }

import java.time.Instant

/**
 * Type safe API example
 */
object StudentZioDynamoDbExampleWithOptics extends App {

  val enrollmentDateTyped: ProjectionExpression[Student, Option[Instant]] = enrollmentDate

  private val program = for {
    _ <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
           AttributeDefinition.attrDefnString("email"),
           AttributeDefinition.attrDefnString("subject")
         ).execute
    _ <- batchWriteFromStream(Stream(avi, adam)) { student =>
           put("student", student)
         }.runDrain
    _ <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _ <- batchReadFromStream("student", Stream(avi, adam))(s => primaryKey(s.email, s.subject))
           .tap(student => console.putStrLn(s"student=$student"))
           .runDrain
    _ <- scanAll[Student]("student").filter {
           enrollmentDate === Some(
             enrolDate
           ) && payment === Payment.CreditCard
           // TODO: Avi - "&& Elephant.email === "elephant@gmail.com"" fails to compile as expected
         }.execute
           .map(_.runCollect)
    _ <- queryAll[Student]("student")
           .filter(                              // TODO: Avi - "&& Elephant.email === "elephant@gmail.com"" fails to compile as expected
             enrollmentDate === Some(enrolDate) && payment === Payment.CreditCard
           )
           .whereKey(email === "avi@gmail.com" && subject === "maths" /* && Elephant.email === "elephant@gmail.com" */ )
           .execute
           .map(_.runCollect)
    _ <- put[Student]("student", avi)
           .where(
             enrollmentDate === Some(
               enrolDate
             ) && email === "avi@gmail.com" && payment === Payment.CreditCard
           )
           .execute
    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           enrollmentDate.set(Some(enrolDate2)) + payment.set(Payment.PayPal) + address
             .set(
               Some(Address("line1", "postcode1"))
             )
         }.execute
    _ <- delete("student", primaryKey("adam@gmail.com", "english"))
           .where(
             enrollmentDate === Some(
               enrolDate
             ) && payment === Payment.CreditCard // && zio.dynamodb.examples.Elephant.email === "elephant@gmail.com"
           )
           .execute
    _ <- scanAll[Student]("student").execute
           .tap(_.tap(student => console.putStrLn(s"scanAll - student=$student")).runDrain)
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
