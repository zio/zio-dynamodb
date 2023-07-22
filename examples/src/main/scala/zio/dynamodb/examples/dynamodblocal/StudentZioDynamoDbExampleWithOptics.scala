package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.stream.ZStream
import zio.{ Console, ZIOAppDefault }

import java.time.Instant

/**
 * Type safe API example
 */
object StudentZioDynamoDbExampleWithOptics extends ZIOAppDefault {

  val enrollmentDateTyped: ProjectionExpression[Student, Option[Instant]] = enrollmentDate

  private val program = for {
    _ <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
           AttributeDefinition.attrDefnString("email"),
           AttributeDefinition.attrDefnString("subject")
         ).execute
    _ <- batchWriteFromStream(ZStream(avi, adam)) { student =>
           put("student", student)
         }.runDrain
    _ <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _ <- batchReadFromStream("student", ZStream(avi, adam))(s => primaryKey(s.email, s.subject))
           .tap(errorOrStudent => Console.printLine(s"student=$errorOrStudent"))
           .runDrain
    _ <- scanAll[Student]("student").filter {
           enrollmentDate === Some(
             enrolDate
           ) && payment === Payment.CreditCard
         }.execute
           .map(_.runCollect)
    _ <- queryAll[Student]("student")
           .filter(
             enrollmentDate === Some(enrolDate) && payment === Payment.CreditCard
           )
           .whereKey(email.primaryKey === "avi@gmail.com" && subject.sortKey === "maths")
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
             ) && payment === Payment.CreditCard // && zio.dynamodb.amples.Elephant.email === "elephant@gmail.com"
           )
           .execute
    _ <- scanAll[Student]("student").execute
           .tap(_.tap(student => Console.printLine(s"scanAll - student=$student")).runDrain)
    _ <- deleteTable("student").execute
  } yield ()

  override def run = program.provide(layer)
}
