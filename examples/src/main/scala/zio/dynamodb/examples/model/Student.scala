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
  groups: Set[String] = Set.empty[String]
)

object Student extends DefaultJavaTimeSchemas {
  implicit val schema                                                                                               = DeriveSchema.gen[Student]
  val (email, subject, enrollmentDate, payment, altPayment, studentNumber, collegeName, address, addresses, groups) =
    ProjectionExpression.accessors[Student]
}
