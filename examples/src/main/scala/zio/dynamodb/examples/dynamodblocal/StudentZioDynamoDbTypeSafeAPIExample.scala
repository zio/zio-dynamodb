package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.stream.ZStream
import zio.{ Console, ZIOAppDefault }

/**
 * Type safe API example
 */
object StudentZioDynamoDbTypeSafeAPIExample extends ZIOAppDefault {

  val ce1: ConditionExpression.Operand.Size[Student, String]            = collegeName.size // compiles
  val ce2: ConditionExpression[Student]                                 = ce1 <= 2
  val elephantCe: ConditionExpression[Elephant]                         = Elephant.email === "elephant@gmail.com"
  val elephantAction: Action[Elephant]                                  = Elephant.email.set("ele@gmail.com")
  val ceStudentWithElephant: ConditionExpression[Student with Elephant] = ce2 && elephantCe

  private val program = for {
    _     <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
               AttributeDefinition.attrDefnString("email"),
               AttributeDefinition.attrDefnString("subject")
             ).execute
    _     <- batchWriteFromStream(ZStream(avi, adam)) { student =>
               put("student", student)
             }.runDrain
    /*
  - must be a scalar
    - mandatory Partition Key and Optional sort key
    - String, Number or Binary
  - defined once, used many times
    problem with `Student.email.set("x"), Student.subject.set("y")` is that we can not create a function for this to reuse
    Student.email.partitionKey === "x"                                    // qualifies as a both a PK and a WhereKeyExpression
    Student.email.partitionKey === "x" && Student.subject.sortKey === "y" // qualifies as a both a PK and a WhereKeyExpression
    Student.email.partitionKey === "x" && Student.subject.sortKey > 1     // qualifies as only a WhereKeyExpression
     */
    found <- get("table", Student.email.set("x"), Student.subject.set("y")).execute
    _      = println(s"XXXXXXXX found=$found")

    _ <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _ <- batchReadFromStream("student", ZStream(avi, adam))(student => primaryKey(student.email, student.subject))
           .tap(errorOrStudent => Console.printLine(s"student=$errorOrStudent"))
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
             enrollmentDate === Some(enrolDate) && payment === Payment.PayPal //&& elephantCe
           )
           .whereKey(email === "avi@gmail.com" && subject === "maths" /* && elephantCe */ )
           .execute
           .map(_.runCollect)
    _ <- put[Student]("student", avi)
           .where(
             enrollmentDate === Some(
               enrolDate
             ) && email === "avi@gmail.com" && payment === Payment.CreditCard && $(
               "payment"
             ) === "CreditCard" /* && elephantCe */
           )
           .execute
    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           altPayment.set(Payment.PayPal) + addresses.prependList(List(Address("line0", "postcode0"))) + studentNumber
             .add(1000) + groups.addSet(Set("group3")) // + elephantAction
         }.execute

    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           altPayment.set(Payment.PayPal) + addresses.appendList(List(Address("line3", "postcode3"))) + groups
             .deleteFromSet(Set("group1"))
         }.execute

    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           enrollmentDate.setIfNotExists(Some(enrolDate2)) + payment.set(altPayment) + address
             .set(
               Some(Address("line1", "postcode1"))
             ) // + elephantAction
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
           .tap(_.tap(student => Console.printLine(s"scanAll - student=$student")).runDrain)
    _ <- deleteTable("student").execute
  } yield ()

  override def run = program.provide(layer).exitCode
}
