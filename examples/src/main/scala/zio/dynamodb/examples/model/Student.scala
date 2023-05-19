package zio.dynamodb.examples.model

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.{ PrimaryKey, ProjectionExpression }
import zio.schema.DeriveSchema

import java.time.Instant
import zio.schema.Schema

@enumOfCaseObjects
sealed trait Payment

object Payment {
  case object DebitCard extends Payment

  case object CreditCard extends Payment

  case object PayPal extends Payment

  implicit val schema: Schema.Enum3[DebitCard.type, CreditCard.type, PayPal.type, Payment] = DeriveSchema.gen[Payment]
}

// final case class Location(country: String, district: String)
// object Location {

// }

final case class Address(addr1: String = "UK", postcode: String = "DE24 3JG", number: Int = 1)

object Address {
  implicit val schema: Schema.CaseClass3[String, String, Int, Address] =
    DeriveSchema.gen[Address]

  val (addr1, postcode, number) = ProjectionExpression.accessors[Address]
}

final case class Vehicle(registration: String, make: String)

object Vehicle {
  implicit val schema: Schema.CaseClass2[String, String, Vehicle] = DeriveSchema.gen[Vehicle]

  val (registration, make) = ProjectionExpression.accessors[Vehicle]
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
  version: Int = 0,
  primary: Address = Address(),
  addressMap: Map[String, Address] = Map.empty[String, Address]
)

object Student {
  implicit val schema: Schema.CaseClass13[
    String,
    String,
    Option[Instant],
    Payment,
    Payment,
    Int,
    String,
    Option[Address],
    List[Address],
    Set[String],
    Int,
    Address,
    Map[String, Address],
    Student
  ] = DeriveSchema.gen[Student]
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
    version,
    primary,
    addressMap
  ) =
    ProjectionExpression.accessors[Student]

  def primaryKey(email: String, subject: String): PrimaryKey = PrimaryKey("email" -> email, "subject" -> subject)

  val enrolDate  = Instant.parse("2021-03-20T01:39:33Z")
  val enrolDate2 = Instant.parse("2022-03-20T01:39:33Z")

  val avi  = Student(
    email = "avi@gmail.com",
    subject = "maths",
    enrollmentDate = Some(enrolDate),
    payment = Payment.DebitCard,
    altPayment = Payment.CreditCard,
    studentNumber = 1,
    collegeName = "college1",
    address = None,
    addresses = List(Address("line2", "postcode2")),
    groups = Set("group1", "group2")
  )
  val adam = Student(
    email = "adam@gmail.com",
    subject = "english",
    enrollmentDate = Some(enrolDate),
    payment = Payment.CreditCard,
    altPayment = Payment.DebitCard,
    studentNumber = 2,
    collegeName = "college1",
    address = None,
    addresses = List.empty,
    groups = Set(
      "group1",
      "group2"
    )
  )

}
