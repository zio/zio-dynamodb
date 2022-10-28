package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.stream.ZStream
import zio.{ console, App, ExitCode, URIO }

/**
 * Type safe API example
 */
object StudentZioDynamoDbTypeSafeAPIExample extends App {

  val ce1: ConditionExpression.Operand.Size[Student, String]            = collegeName.size // compiles
  val ce2: ConditionExpression[Student]                                 = ce1 <= 2
  val ceElephant: ConditionExpression[Elephant]                         = Elephant.email === "XXXX"
  val ceStudentWithElephant: ConditionExpression[Student with Elephant] = ce2 && ceElephant

  private val program = for {
    _ <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
           AttributeDefinition.attrDefnString("email"),
           AttributeDefinition.attrDefnString("subject")
         ).execute
    _ <- batchWriteFromStream(ZStream(avi, adam)) { student =>
           put("student", student)
         }.runDrain
    _ <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _ <- batchReadFromStream("student", ZStream(avi, adam))(student => primaryKey(student.email, student.subject))
           .tap(student => console.putStrLn(s"student=$student"))
           .runDrain
    _ <- scanAll[Student]("student")
           .parallel(10)
           .filter {
             enrollmentDate === Some(enrolDate) && payment === Payment.PayPal && payment === altPayment && $(
               "payment"
             ) === "PayPal"
           }
           .execute
           .map(_.runCollect)
    _ <- queryAll[Student]("student")
           .filter(
             enrollmentDate === Some(enrolDate) && payment === Payment.PayPal
           )
           .whereKey(email === "avi@gmail.com" && subject === "maths")
           .execute
           .map(_.runCollect)
    _ <- put[Student]("student", avi)
           .where(
             enrollmentDate === Some(
               enrolDate
             ) && email === "avi@gmail.com" && payment === Payment.CreditCard && $(
               "payment"
             ) === "CreditCard"
           )
           .execute
    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           altPayment.set(Payment.PayPal) + addresses.prependList(List(Address("line0", "postcode0"))) + studentNumber
             .add(1000) + groups.addSet(Set("group3"))
         }.execute

    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           altPayment.set(Payment.PayPal) + addresses.appendList(List(Address("line3", "postcode3"))) + groups
             .deleteFromSet(Set("group1"))
         }.execute

    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           enrollmentDate.setIfNotExists(Some(enrolDate2)) + payment.set(altPayment) + address
             .set(
               Some(Address("line1", "postcode1"))
             )
         }.execute
    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           addresses.remove(1)
         }.execute
    _ <-
      delete("student", primaryKey("adam@gmail.com", "english"))
        .where(
          (enrollmentDate === Some(enrolDate) && payment <> Payment.PayPal && studentNumber
            .between(1, 3) && groups.contains("group1") && collegeName.contains(
            "college1"
          ) && collegeName.size > 1 && groups.size > 1 /* && zio.dynamodb.examples.Elephant.email === "elephant@gmail.com" */ )
        )
        .execute
    _ <- scanAll[Student]("student")
           .filter[Student](payment.in(Payment.PayPal) && payment.inSet(Set(Payment.PayPal)))
           .execute
           .tap(_.tap(student => console.putStrLn(s"scanAll - student=$student")).runDrain)
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
