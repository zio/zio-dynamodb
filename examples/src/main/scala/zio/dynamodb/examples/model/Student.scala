package zio.dynamodb.examples.model

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.ProjectionExpression
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }

import java.time.Instant

@enumOfCaseObjects
sealed trait Payment

object Payment {
  final case object DebitCard extends Payment

  final case object CreditCard extends Payment

  final case object PayPal extends Payment

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
  groups: Set[String] = Set.empty[String],
  version: Int = 0
)

object Student extends DefaultJavaTimeSchemas {
  implicit val schema = DeriveSchema.gen[Student]
  val (
    email,
    subject,
    enrollmentDate,
    payment,
    altPayment,
    studentNumber,
    collegeName,
    address,
    addresses,
    groups,
    version
  )                   =
    ProjectionExpression.accessors[Student]

  val enrolDate  = Instant.parse("2021-03-20T01:39:33Z")
  val enrolDate2 = Instant.parse("2022-03-20T01:39:33Z")

  val avi  = Student(
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
  val adam = Student(
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

}
